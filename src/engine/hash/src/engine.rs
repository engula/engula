// Copyright 2021 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use engula_journal::{Event, Stream};
use engula_kernel::{
    Kernel, KernelUpdate, ResultStream, Sequence, UpdateAction, Version, VersionUpdate,
};
use engula_storage::Bucket;
use futures::TryStreamExt;
use tokio::sync::Mutex;

use crate::{
    format::{self, Timestamp},
    memtable::Memtable,
    table_builder::TableBuilder,
    table_reader::TableReader,
    Error, Result,
};

pub struct Engine<K: Kernel> {
    ts: Mutex<Timestamp>,
    vset: VersionSet<K>,
}

impl<K: Kernel> Engine<K> {
    pub async fn new(kernel: K) -> Result<Self> {
        let vset = VersionSet::new(kernel).await?;
        Ok(Self {
            ts: Mutex::new(0),
            vset,
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.vset.get(key).await
    }

    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut ts = self.ts.lock().await;
        *ts += 1;
        self.vset.set(*ts, key, value).await
    }
}

const MEMTABLE_SIZE: usize = 4 * 1024;

#[derive(Clone)]
struct VersionSet<K: Kernel> {
    kernel: K,
    bucket: K::Bucket,
    number: Arc<AtomicU64>,
    current: Arc<Mutex<Arc<EngineVersion<K>>>>,
}

impl<K: Kernel> VersionSet<K> {
    async fn new(kernel: K) -> Result<Self> {
        let stream = kernel.stream().await?;
        let bucket = kernel.bucket().await?;
        let version = kernel.current_version().await?;
        let version_updates = kernel.version_updates(version.sequence).await;

        let current = EngineVersion::new(stream, bucket.clone(), version).await?;
        let vset = Self {
            kernel,
            bucket,
            number: Arc::new(AtomicU64::new(0)),
            current: Arc::new(Mutex::new(Arc::new(current))),
        };

        // Starts a task to update versions.
        {
            let vset = vset.clone();
            tokio::spawn(async move {
                Self::handle_version_updates(vset, version_updates)
                    .await
                    .unwrap();
            });
        }

        Ok(vset)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let current = self.current_version().await;
        current.get(key).await
    }

    pub async fn set(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let current = self.current_version().await;
        current.set(ts, key, value).await?;
        if let Some((imm, version)) = current.should_flush().await {
            self.install_version(version).await;
            let vset = self.clone();
            tokio::spawn(async move {
                Self::flush(vset, imm).await.unwrap();
            });
        }
        Ok(())
    }

    async fn flush(self: Self, imm: Arc<Memtable>) -> Result<()> {
        let number = self.number.fetch_add(1, Ordering::SeqCst);
        let object = format!("{}", number);
        let writer = self.bucket.new_sequential_writer(&object).await?;

        let mut table_builder = TableBuilder::new(writer);
        for (key, value) in imm.iter().await {
            table_builder.add(&key, &value).await?;
        }
        table_builder.finish().await?;

        let mut update = KernelUpdate::default();
        update.add_object(object);
        self.kernel.install_update(update).await?;
        Ok(())
    }

    async fn current_version(&self) -> Arc<EngineVersion<K>> {
        let current = self.current.lock().await;
        current.clone()
    }

    async fn install_version(&self, version: EngineVersion<K>) {
        let mut current = self.current.lock().await;
        *current = Arc::new(version);
    }

    async fn handle_version_updates(
        self: Self,
        mut updates: ResultStream<Arc<VersionUpdate>>,
    ) -> Result<()> {
        while let Some(update) = updates.try_next().await? {
            let current = self.current.lock().await.clone();
            let version = current.install_update(update).await?;
            self.install_version(version).await;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EngineVersion<K: Kernel> {
    stream: K::Stream,
    bucket: K::Bucket,
    sequence: Sequence,
    mem: Arc<Memtable>,
    imm: VecDeque<Arc<Memtable>>,
    tables: Vec<Arc<TableReader>>,
}

impl<K: Kernel> EngineVersion<K> {
    async fn new(stream: K::Stream, bucket: K::Bucket, version: Arc<Version>) -> Result<Self> {
        let mut tables = Vec::new();
        for object in &version.objects {
            let reader = bucket.new_sequential_reader(object).await?;
            let table_reader = TableReader::new(reader).await?;
            tables.push(Arc::new(table_reader));
        }
        Ok(Self {
            stream,
            bucket,
            sequence: version.sequence,
            mem: Arc::new(Memtable::new(0)),
            imm: VecDeque::new(),
            tables,
        })
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.mem.get(key).await {
            return Ok(Some(value));
        }
        for imm in self.imm.iter().rev() {
            if let Some(value) = imm.get(key).await {
                return Ok(Some(value));
            }
        }
        for table in self.tables.iter().rev() {
            match table.get(key).await {
                Ok(Some(value)) => return Ok(Some(value)),
                Ok(None) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }

    async fn set(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let event = Event {
            ts: ts.into(),
            data: format::encode_record(&key, &value),
        };
        self.stream.append_event(event).await?;
        self.mem.set(ts, key, value).await;
        Ok(())
    }

    async fn should_flush(&self) -> Option<(Arc<Memtable>, EngineVersion<K>)> {
        if self.mem.approximate_size().await > MEMTABLE_SIZE {
            let mut version = self.clone();
            let mem = version.mem.clone();
            let last_ts = mem.last_update_timestamp().await;
            version.imm.push_back(mem.clone());
            version.mem = Arc::new(Memtable::new(last_ts));
            Some((mem, version))
        } else {
            None
        }
    }

    async fn install_update(&self, update: Arc<VersionUpdate>) -> Result<EngineVersion<K>> {
        // Makes sure we don't miss updates.
        assert_eq!(self.sequence + 1, update.sequence);

        let mut version = self.clone();
        for action in &update.actions {
            match action {
                UpdateAction::AddObject(object) => {
                    // For now, we assume that this object is flushed from the oldest immtable.
                    let reader = version.bucket.new_sequential_reader(object).await?;
                    let table_reader = TableReader::new(reader).await?;
                    version.tables.push(Arc::new(table_reader));
                    let imm = version.imm.pop_front().unwrap();
                    let last_ts = imm.last_update_timestamp().await;
                    version.stream.release_events(last_ts.into()).await?;
                }
                _ => return Err(Error::Unsupported(format!("{:?}", action))),
            }
        }
        version.sequence = update.sequence;
        Ok(version)
    }
}
