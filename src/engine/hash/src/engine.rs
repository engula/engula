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
    sync::Arc,
};

use engula_kernel::{
    Bucket, Event, Kernel, KernelUpdate, ResultStream, Sequence, Stream, Version, VersionUpdate,
};
use futures::TryStreamExt;
use tokio::sync::Mutex;

use crate::{
    codec::{self, Timestamp, Value},
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
    last_timestamp: Arc<Mutex<Timestamp>>,
    last_object_number: Arc<Mutex<u64>>,
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
            last_timestamp: Arc::new(Mutex::new(0)),
            last_object_number: Arc::new(Mutex::new(0)),
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
                let (key, value) = codec::decode_record(&event.data)?;
                current.insert(ts, key, value).await;
            }
        }
        *self.last_timestamp.lock().await = ts;
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let current = self.current_version().await;
        let value = match current.get(key).await? {
            Some(Value::Put(value)) => Some(value),
            Some(Value::Deletion) => None,
            None => None,
        };
        Ok(value)
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write(key, Value::Put(value)).await
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write(key, Value::Deletion).await
    }

    async fn write(&self, key: Vec<u8>, value: Value) -> Result<()> {
        let mut ts = self.last_timestamp.lock().await;
        *ts += 1;

        let event = Event {
            ts: (*ts).into(),
            data: codec::encode_record(&key, &value),
        };
        self.stream.append_event(event).await?;

        let current = self.current_version().await;
        current.insert(*ts, key, value).await;

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
        let mut number = self.last_object_number.lock().await;
        (*number) += 1;

        let object = format!("{}", number);
        let writer = self.bucket.new_sequential_writer(&object).await?;

        let mut table_builder = TableBuilder::new(writer);
        for (key, value) in imm.iter().await {
            table_builder.add(&key, &value).await?;
        }
        table_builder.finish().await?;

        let mut update = KernelUpdate::default();
        let last_ts = encode_u64_meta(imm.last_update_timestamp().await);
        update.add_meta(LAST_TIMESTAMP, last_ts);
        let last_number = encode_u64_meta(*number);
        update.add_meta(LAST_OBJECT_NUMBER, last_number);
        update.add_object(object);
        self.kernel.apply_update(update).await?;
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
    last_object_number: u64,
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
        let last_timestamp = decode_u64_meta(&version.meta, LAST_TIMESTAMP)?.unwrap_or(0);
        let last_object_number = decode_u64_meta(&version.meta, LAST_OBJECT_NUMBER)?.unwrap_or(0);
        Ok(Self {
            bucket,
            last_sequence: version.sequence,
            last_timestamp,
            last_object_number,
            mem: Arc::new(Memtable::new(0)),
            imm: VecDeque::new(),
            tables,
        })
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Value>> {
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

    async fn insert(&self, ts: Timestamp, key: Vec<u8>, value: Value) {
        self.mem.insert(ts, key, value).await;
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
        if let Some(value) = decode_u64_meta(&update.add_meta, LAST_TIMESTAMP)? {
            version.last_timestamp = value;
        }
        if let Some(value) = decode_u64_meta(&update.add_meta, LAST_OBJECT_NUMBER)? {
            version.last_object_number = value;
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
const LAST_TIMESTAMP: &str = "last_timestamp";
const LAST_OBJECT_NUMBER: &str = "last_object_number";

fn encode_u64_meta(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn decode_u64_meta(map: &HashMap<String, Vec<u8>>, name: &str) -> Result<Option<u64>> {
    if let Some(buf) = map.get(name) {
        let buf = buf.as_slice().try_into().map_err(Error::corrupted)?;
        Ok(Some(u64::from_be_bytes(buf)))
    } else {
        Ok(None)
    }
}
