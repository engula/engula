// Copyright 2022 The Engula Authors.
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

use std::{io::Result, path::PathBuf};

mod store;
use store::{BlockHandle, DiskStore};

mod table;
use table::HashTable;

pub struct DiskOptions {
    mem_capacity: usize,
    disk_capacity: usize,
    file_size: usize,
    file_buffer_size: usize,
}

pub struct DiskCache {
    table: HashTable,
    store: DiskStore,
}

impl DiskCache {
    pub async fn new(root: impl Into<PathBuf>, options: DiskOptions) -> Result<DiskCache> {
        let root = root.into();
        let table = HashTable::with_mem_capacity(options.mem_capacity);
        let store = DiskStore::open(root, options).await?;
        Ok(Self { table, store })
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut entry = self.table.get(key);
        while let Some(ent) = entry {
            let handle = ent.as_handle();
            if let Some(value) = self.store.read(key, &handle).await? {
                return Ok(Some(value));
            }
            entry = self.table.next(ent.next);
        }
        Ok(None)
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = self.store.write(key, value).await?;
        self.table.insert(key, handle);
        Ok(())
    }
}
