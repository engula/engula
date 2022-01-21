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

use std::sync::Arc;

use engula_journal::{StreamReader, StreamWriter};
use engula_kernel::{Kernel, KernelUpdateBuilder, UpdateWriter};
use tokio::sync::Mutex;

use crate::{
    codec::Timestamp, scanner::Scanner, store::Store, Options, ReadOptions, Result, Snapshot,
    WriteBatch, WriteOptions, DEFAULT_NAME,
};

pub struct Database<K: Kernel> {
    inner: Mutex<Inner<K>>,
}

struct Inner<K: Kernel> {
    kernel: Arc<K>,
    stream_writer: K::StreamWriter,
    last_ts: Timestamp,
    store: Store,
}

impl<K: Kernel> Database<K>
where
    K: Kernel + Send + Sync + 'static,
    K::UpdateReader: Send,
    K::UpdateWriter: Send,
    K::RandomReader: Sync + Send + Unpin,
    K::SequentialWriter: Send + Unpin,
{
    pub async fn open(options: Options, kernel: Arc<K>) -> Result<Self> {
        let mut update_writer = kernel.new_update_writer().await?;
        let stream_writer = match kernel.new_stream_writer(DEFAULT_NAME).await {
            Ok(stream) => stream,
            Err(engula_kernel::Error::NotFound(_)) => {
                // Initializes the kernel.
                let update = KernelUpdateBuilder::default()
                    .add_stream(DEFAULT_NAME)
                    .add_bucket(DEFAULT_NAME)
                    .build();
                update_writer.append(update).await?;
                kernel.new_stream_writer(DEFAULT_NAME).await?
            }
            Err(err) => return Err(err.into()),
        };
        let store = Store::new(options, kernel.clone());
        let inner = Inner {
            kernel,
            stream_writer,
            last_ts: 0,
            store,
        };
        let db = Database {
            inner: Mutex::new(inner),
        };
        db.recover().await?;
        Ok(db)
    }

    async fn recover(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let mut stream_reader = inner.kernel.new_stream_reader(DEFAULT_NAME).await?;
        while let Some((sequence, event)) = stream_reader.try_next().await? {
            let batch = WriteBatch::decode_from(&event)?;
            assert!(batch.timestamp() >= inner.last_ts);
            inner.last_ts = batch.timestamp() + batch.writes.len() as u64 - 1;
            inner.store.write(batch, sequence).await;
        }
        Ok(())
    }

    pub async fn write(&self, _options: &WriteOptions, mut batch: WriteBatch) -> Result<()> {
        if batch.writes.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().await;
        batch.set_timestamp(inner.last_ts + 1);
        inner.last_ts += batch.writes.len() as u64;
        let data = batch.encode_to_vec();
        let sequence = inner.stream_writer.append(data).await?;
        inner.store.write(batch, sequence).await;
        Ok(())
    }

    pub async fn get(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().await;
        inner.store.get(options, key).await
    }

    pub async fn scan(&self, options: &ReadOptions) -> Scanner {
        let inner = self.inner.lock().await;
        inner.store.scan(options).await
    }

    pub async fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock().await;
        Snapshot { ts: inner.last_ts }
    }
}
