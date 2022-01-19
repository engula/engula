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

use engula_kernel::{BucketUpdateBuilder, Kernel, KernelUpdateBuilder, UpdateWriter};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    codec::{FlushDesc, TableDesc, UpdateDesc, MAX_TIMESTAMP},
    memtable::Memtable,
    scan::Scan,
    table::{TableBuilder, TableBuilderOptions},
    Result, DEFAULT_NAME,
};

pub struct FlushScheduler {
    tx: mpsc::Sender<Arc<Memtable>>,
}

impl FlushScheduler {
    pub fn new<K>(kernel: Arc<K>) -> Self
    where
        K: Kernel + Send + Sync + 'static,
        K::UpdateWriter: Send,
        K::SequentialWriter: Send + Unpin,
    {
        let (tx, rx) = mpsc::channel(8);
        {
            tokio::spawn(async move {
                Self::handle_flushes(kernel, rx).await.unwrap();
            });
        }
        Self { tx }
    }

    pub async fn submit(&self, mem: Arc<Memtable>) {
        self.tx.send(mem).await.map_err(|_| "send error").unwrap();
    }

    pub async fn handle_flushes<K>(
        kernel: Arc<K>,
        mut rx: mpsc::Receiver<Arc<Memtable>>,
    ) -> Result<()>
    where
        K: Kernel,
        K::UpdateWriter: Send,
        K::SequentialWriter: Send + Unpin,
    {
        let mut update_writer = kernel.new_update_writer().await?;
        let mut last_ts = 0;

        while let Some(mem) = rx.recv().await {
            assert!(mem.last_timestamp() > last_ts);
            last_ts = mem.last_timestamp();

            let object_name = Uuid::new_v4().to_string();
            let writer = kernel
                .new_sequential_writer(DEFAULT_NAME, &object_name)
                .await?;
            let topts = TableBuilderOptions::default();
            let mut builder = TableBuilder::new(topts, writer);
            let mut scanner = mem.scan(MAX_TIMESTAMP);
            scanner.seek_to_first();
            while scanner.valid() {
                builder.add(scanner.key(), scanner.value()).await?;
                scanner.next();
            }
            let table_size = builder.finish().await?;

            let desc = UpdateDesc::Flush(FlushDesc {
                memtable_id: mem.id().to_owned(),
            });
            let table_desc = TableDesc { table_size };
            let bucket_update = BucketUpdateBuilder::default()
                .add_object(object_name, table_desc.encode_to_vec())
                .build();
            let kernel_update = KernelUpdateBuilder::default()
                .put_meta("desc", desc.encode_to_vec())
                .update_bucket(DEFAULT_NAME, bucket_update)
                .build();
            update_writer.append(kernel_update).await?;
        }
        Ok(())
    }
}
