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

use engula_futures::io::RandomRead;
use engula_journal::{StreamReader, StreamWriter};
use engula_kernel::{Kernel, KernelUpdateBuilder, UpdateWriter};
use tokio::sync::Mutex;

use crate::{codec::Timestamp, scan::Scan, version::Scanner, Result, WriteBatch};

pub struct Database<K: Kernel> {
    inner: Mutex<Inner<K>>,
}

struct Inner<K: Kernel> {
    kernel: K,
    stream_writer: K::StreamWriter,
    _update_reader: K::UpdateReader,
    _update_writer: K::UpdateWriter,
    last_ts: Timestamp,
}

const DEFAULT_NAME: &str = "default";

impl<K: Kernel> Database<K> {
    pub async fn open(kernel: K) -> Result<Self> {
        let update_reader = kernel.new_update_reader().await?;
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
        let inner = Inner {
            kernel,
            stream_writer,
            _update_reader: update_reader,
            _update_writer: update_writer,
            last_ts: 0,
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
        while let Some(event) = stream_reader.try_next().await? {
            let batch = WriteBatch::decode_from(&event)?;
            assert!(batch.timestamp() > inner.last_ts);
            inner.last_ts = batch.timestamp();
        }
        Ok(())
    }

    pub async fn write(&self, _options: &WriteOptions, mut batch: WriteBatch) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.last_ts += 1;
        batch.set_timestamp(inner.last_ts);
        let data = batch.encode_to_vec();
        inner.stream_writer.append(data).await?;
        Ok(())
    }

    pub async fn get(&self, _options: &ReadOptions, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!();
    }

    pub async fn scan<'a, S, R>(&'a self, _options: &ReadOptions) -> Scanner<'a, S, R>
    where
        S: Scan,
        R: RandomRead + 'a,
    {
        todo!();
    }

    pub fn snapshot(&self) -> Snapshot {
        todo!();
    }
}

pub struct Snapshot {}

#[derive(Default)]
pub struct ReadOptions {
    _snapshot: Option<Snapshot>,
}

#[derive(Default)]
pub struct WriteOptions {}
