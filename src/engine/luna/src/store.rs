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

use std::{collections::VecDeque, sync::Arc};

use engula_kernel::{Kernel, UpdateReader};
use tokio::sync::Mutex;

use crate::{
    codec::{FlushDesc, InternalComparator, TableDesc, UpdateDesc},
    flush_scheduler::FlushScheduler,
    level::{LevelState, TableState},
    memtable::Memtable,
    scanner::Scanner,
    table::TableReader,
    version::Version,
    Options, ReadOptions, Result, WriteBatch, DEFAULT_NAME,
};

pub struct Store {
    inner: Arc<Mutex<Inner>>,
    options: Options,
    flush_scheduler: FlushScheduler,
}

impl Store {
    pub fn new<K>(options: Options, kernel: Arc<K>) -> Self
    where
        K: Kernel + Send + Sync + 'static,
        K::UpdateReader: Send,
        K::UpdateWriter: Send,
        K::RandomReader: Send + Sync + Unpin,
        K::SequentialWriter: Send + Unpin,
    {
        let inner = Arc::new(Mutex::new(Inner::new()));
        {
            let inner = inner.clone();
            let kernel = kernel.clone();
            tokio::spawn(async move {
                Self::handle_updates(kernel, inner).await.unwrap();
            });
        }
        Self {
            inner,
            options,
            flush_scheduler: FlushScheduler::new(kernel),
        }
    }

    pub async fn get(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().await;
        Ok(inner.mem.get(options.snapshot.ts, key))
    }

    pub async fn scan(&self, options: &ReadOptions) -> Scanner {
        let inner = self.inner.lock().await;
        let scanner = inner.vset.back().unwrap().scan(options);
        Scanner::new(scanner, options.snapshot.ts)
    }

    pub async fn write(&self, batch: WriteBatch) {
        let mut inner = self.inner.lock().await;
        inner.mem.write(batch);
        if inner.mem.estimated_size() >= self.options.memtable_size {
            let imm = inner.switch_memtable();
            self.flush_scheduler.submit(imm).await;
        }
    }

    async fn handle_updates<K>(kernel: Arc<K>, inner: Arc<Mutex<Inner>>) -> Result<()>
    where
        K: Kernel,
        K::RandomReader: Send + Sync + Unpin + 'static,
    {
        let mut update_reader = kernel.new_update_reader().await?;
        loop {
            let (_, update) = update_reader.wait_next().await?;
            if let Some(meta) = update.put_meta.get("desc") {
                let UpdateDesc::Flush(flush) = UpdateDesc::decode_from(meta)?;
                let bucket_update = update.update_buckets.get(DEFAULT_NAME).unwrap();
                let mut table_states = Vec::new();
                for (object_name, object_meta) in &bucket_update.add_objects {
                    let table_desc = TableDesc::decode_from(object_meta)?;
                    let reader = kernel.new_random_reader(DEFAULT_NAME, object_name).await?;
                    let table_reader = TableReader::open(
                        InternalComparator {},
                        Arc::new(Box::new(reader)),
                        table_desc.table_size as usize,
                    )
                    .await?;
                    let table_state = TableState {
                        desc: table_desc,
                        reader: table_reader,
                    };
                    table_states.push(Arc::new(table_state));
                }
                let mut inner = inner.lock().await;
                inner.install_flush_update(flush, table_states);
            }
        }
    }
}

struct Inner {
    mem: Arc<Memtable>,
    vset: VecDeque<Arc<Version>>,
}

impl Inner {
    fn new() -> Self {
        let mem = Arc::new(Memtable::new(0));
        let mut current = Version::default();
        current.mem.tables.push(mem.clone());
        let mut vset = VecDeque::new();
        vset.push_back(Arc::new(current));
        Self { mem, vset }
    }

    fn clone_current(&self) -> Version {
        (**self.vset.back().unwrap()).clone()
    }

    fn switch_memtable(&mut self) -> Arc<Memtable> {
        let imm = self.mem.clone();
        self.mem = Arc::new(Memtable::new(imm.last_timestamp()));
        let mut version = self.clone_current();
        version.mem.tables.push(self.mem.clone());
        self.vset.push_back(Arc::new(version));
        imm
    }

    fn install_flush_update(&mut self, flush: FlushDesc, tables: Vec<Arc<TableState>>) {
        let mut version = self.clone_current();
        let index = version
            .mem
            .tables
            .iter()
            .position(|x| x.id() == flush.memtable_id)
            .unwrap();
        version.mem.tables.remove(index);
        let level = LevelState { tables };
        version.base.levels.push(level);
        self.vset.push_back(Arc::new(version));
    }
}
