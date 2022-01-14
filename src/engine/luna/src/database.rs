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

use std::collections::{hash_map, HashMap};

use engula_futures::io::RandomRead;
use tokio::sync::Mutex;

use crate::{scan::Scan, version::Scanner, Collection, Error, Result, WriteBatch};

pub struct Database {
    inner: Mutex<Inner>,
}

pub struct Inner {
    collections: HashMap<String, Collection>,
    next_collection_id: u64,
}

impl Database {
    pub async fn open() -> Result<Self> {
        let inner = Inner {
            collections: HashMap::new(),
            next_collection_id: 0,
        };
        Ok(Database {
            inner: Mutex::new(inner),
        })
    }

    pub async fn collection(&self, name: &str) -> Result<Option<Collection>> {
        let inner = self.inner.lock().await;
        Ok(inner.collections.get(name).cloned())
    }

    pub async fn list_collections(&self) -> Result<HashMap<String, Collection>> {
        let inner = self.inner.lock().await;
        Ok(inner.collections.clone())
    }

    pub async fn create_collection(&self, name: &str) -> Result<Collection> {
        let mut inner = self.inner.lock().await;
        let co = Collection::new(inner.next_collection_id);
        inner.next_collection_id += 1;
        match inner.collections.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(co.clone());
                Ok(co)
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("collection '{}'", ent.key())))
            }
        }
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.collections.remove(name);
        Ok(())
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {}
    }

    pub async fn get(
        &self,
        _options: &ReadOptions,
        _co: &Collection,
        _key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        todo!();
    }

    pub async fn scan<'a, S, R>(
        &'a self,
        _options: &ReadOptions,
        _co: &Collection,
    ) -> Scanner<'a, S, R>
    where
        S: Scan,
        R: RandomRead + 'a,
    {
        todo!();
    }

    pub async fn write(&self, _options: &WriteOptions, _batch: WriteBatch) -> Result<()> {
        todo!();
    }
}

pub struct Snapshot {}

pub struct ReadOptions {
    _snapshot: Snapshot,
}

pub struct WriteOptions {}
