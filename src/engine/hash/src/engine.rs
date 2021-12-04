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
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use engula_journal::{Event, Stream};
use engula_kernel::{Kernel, KernelUpdate, ResultStream, Sequence, Version, VersionUpdate};
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

#[derive(Clone)]
pub struct Engine<K: Kernel> {
    kernel: K,
    stream: K::Stream,
    bucket: K::Bucket,
    current: Arc<Mutex<Arc<EngineVersion<K>>>>,
    last_ts: Arc<Mutex<Timestamp>>,
    last_number: Arc<AtomicU64>,
}

impl<K: Kernel> Engine<K> {
    pub async fn open(kernel: K) -> Result<Self> {
        let stream = kernel.stream().await?;
        let bucket = kernel.bucket().await?;
        let version = kernel.current_version().await?;
        let version_updates = kernel.version_updates(version.sequence).await;

        let current = EngineVersion::open(bucket.clone(), version).await?;
        let engine = Self {
            kernel,
            stream,
            bucket,
            current: Arc::new(Mutex::new(Arc::new(current))),
            last_ts: Arc::new(Mutex::new(0)),
            last_number: Arc::new(AtomicU64::new(0)),
        };
        engine.recover().await?;

        // Starts a task to update versions.
        {
            let engine = engine.clone();
            tokio::spawn(async move {
                Self::handle_version_updates(engine, version_updates)
                    .await
                    .unwrap();
            });
        }

        Ok(engine)
    }

    async fn recover(&self) -> Result<()> {
        let current = self.current_version().await;
        let mut ts = current.last_timestamp;
        let mut stream = self.stream.read_events((ts + 1).into()).await;
        while let Some(events) = stream.try_next().await? {
            for event in events {
                ts += 1;
                let (key, value) = format::decode_record(&event.data)?;
                current.set(ts, key.to_owned(), value.to_owned()).await;
            }
        }
        *self.last_ts.lock().await = ts;
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let current = self.current_version().await;
        current.get(key).await
    }

    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut ts = self.last_ts.lock().await;
        *ts += 1;

        let event = Event {
            ts: (*ts).into(),
            data: format::encode_record(&key, &value),
        };
        self.stream.append_event(event).await?;

        let current = self.current_version().await;
        current.set(*ts, key, value).await;

        if let Some((imm, version)) = current.should_flush().await {
            self.install_version(Arc::new(version)).await;
            let engine = self.clone();
            tokio::spawn(async move {
                Self::flush(engine, imm).await.unwrap();
            });
        }
        Ok(())
    }

    async fn flush(self, imm: Arc<Memtable>) -> Result<()> {
        let number = self.last_number.fetch_add(1, Ordering::SeqCst);
        let object = format!("{}", number);
        let writer = self.bucket.new_sequential_writer(&object).await?;

        let mut table_builder = TableBuilder::new(writer);
        for (key, value) in imm.iter().await {
            table_builder.add(&key, &value).await?;
        }
        table_builder.finish().await?;

        let mut update = KernelUpdate::default();
        let last_ts = encode_last_timestamp(imm.last_update_timestamp().await);
        update.set_meta(last_ts.0, last_ts.1);
        update.add_object(object);
        self.kernel.install_update(update).await?;
        Ok(())
    }

    async fn current_version(&self) -> Arc<EngineVersion<K>> {
        self.current.lock().await.clone()
    }

    async fn install_version(&self, version: Arc<EngineVersion<K>>) {
        *self.current.lock().await = version;
    }

    async fn handle_version_updates(
        self,
        mut updates: ResultStream<Arc<VersionUpdate>>,
    ) -> Result<()> {
        while let Some(update) = updates.try_next().await? {
            let current = self.current.lock().await.clone();
            let version = current.install_update(update).await?;
            let version = Arc::new(version);
            self.install_version(version.clone()).await;
            self.stream
                .release_events(version.last_timestamp.into())
                .await?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EngineVersion<K: Kernel> {
    bucket: K::Bucket,
    last_sequence: Sequence,
    last_timestamp: Timestamp,
    mem: Arc<Memtable>,
    imm: VecDeque<Arc<Memtable>>,
    tables: Vec<Arc<TableReader>>,
}

impl<K: Kernel> EngineVersion<K> {
    async fn open(bucket: K::Bucket, version: Arc<Version>) -> Result<Self> {
        let mut tables = Vec::new();
        for object in &version.objects {
            let reader = bucket.new_sequential_reader(object).await?;
            let table_reader = TableReader::new(reader).await?;
            tables.push(Arc::new(table_reader));
        }
        let last_timestamp = if let Some(ts) = decode_last_timestamp(&version.meta)? {
            ts
        } else {
            0
        };
        Ok(Self {
            bucket,
            last_sequence: version.sequence,
            last_timestamp,
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

    async fn set(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) {
        self.mem.set(ts, key, value).await;
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
        assert_eq!(self.last_sequence + 1, update.sequence);

        let mut version = self.clone();
        version.last_sequence = update.sequence;
        if let Some(ts) = decode_last_timestamp(&update.set_meta)? {
            version.last_timestamp = ts;
        }

        for object in &update.add_objects {
            // We assume that objects are flushed from the oldest immtable to the newest.
            let reader = version.bucket.new_sequential_reader(object).await?;
            let table_reader = TableReader::new(reader).await?;
            version.tables.push(Arc::new(table_reader));
            version.imm.pop_front();
        }

        Ok(version)
    }
}

const MEMTABLE_SIZE: usize = 4 * 1024;
const LAST_TIMESTAMP: &[u8] = b"last_timestamp";

fn encode_last_timestamp(ts: Timestamp) -> (Vec<u8>, Vec<u8>) {
    (LAST_TIMESTAMP.to_vec(), ts.to_be_bytes().to_vec())
}

fn decode_last_timestamp(map: &HashMap<Vec<u8>, Vec<u8>>) -> Result<Option<Timestamp>> {
    if let Some(buf) = map.get(LAST_TIMESTAMP) {
        let buf = buf.as_slice().try_into().map_err(Error::corrupted)?;
        Ok(Some(u64::from_be_bytes(buf)))
    } else {
        Ok(None)
    }
}
