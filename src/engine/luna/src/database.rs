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
    collections::{hash_map, BTreeMap, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{
    collection::{Collection, CollectionId},
    Error, Result,
};

type Memtable = BTreeMap<Vec<u8>, Vec<u8>>;

pub struct Database {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    next_collection_id: CollectionId,
    collections: HashMap<String, Collection>,
    memtable: HashMap<CollectionId, Memtable>,
}

impl Database {
    pub async fn open() -> Result<Self> {
        let inner = Inner {
            next_collection_id: 0,
            collections: HashMap::new(),
            memtable: HashMap::new(),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub async fn collection(&self, name: &str) -> Result<Option<Collection>> {
        let inner = self.inner.lock().await;
        Ok(inner.collections.get(name).cloned())
    }

    pub async fn create_collection(&self, name: &str) -> Result<Collection> {
        let mut inner = self.inner.lock().await;
        let id = inner.next_collection_id;
        match inner.collections.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                let co = Collection::new(id);
                ent.insert(co.clone());
                inner.memtable.insert(id, BTreeMap::new());
                inner.next_collection_id += 1;
                Ok(co)
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("collection '{}'", ent.key())))
            }
        }
    }

    pub async fn txn(&self) -> Result<Txn> {
        Ok(Txn::new(self.inner.clone()))
    }
}

pub struct Txn {
    inner: Arc<Mutex<Inner>>,
    mutations: Vec<(CollectionId, Op)>,
}

enum Op {
    Put((Vec<u8>, Vec<u8>)),
    Delete(Vec<u8>),
}

impl Txn {
    fn new(inner: Arc<Mutex<Inner>>) -> Self {
        Self {
            inner,
            mutations: Vec::new(),
        }
    }

    pub async fn get(&self, co: &Collection, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().await;
        match inner.memtable.get(&co.id()) {
            Some(table) => Ok(table.get(key).cloned()),
            None => Err(Error::InvalidArgument(format!(
                "collection id {} doesn't exist",
                co.id()
            ))),
        }
    }

    pub fn put(&mut self, co: &Collection, key: Vec<u8>, value: Vec<u8>) {
        let op = Op::Put((key, value));
        self.mutations.push((co.id(), op));
    }

    pub fn delete(&mut self, co: &Collection, key: Vec<u8>) {
        let op = Op::Delete(key);
        self.mutations.push((co.id(), op));
    }

    pub async fn commit(self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        for mu in self.mutations {
            if let Some(table) = inner.memtable.get_mut(&mu.0) {
                match mu.1 {
                    Op::Put(ent) => {
                        table.insert(ent.0, ent.1);
                    }
                    Op::Delete(ent) => {
                        table.remove(&ent);
                    }
                }
            } else {
                return Err(Error::InvalidArgument(format!(
                    "collection id {} doesn't exist",
                    mu.0
                )));
            }
        }
        Ok(())
    }
}
