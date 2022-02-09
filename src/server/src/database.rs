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

use std::{collections::HashMap, sync::Arc};

use engula_apis::*;
use tokio::sync::Mutex;

use crate::{Error, Result};

#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<Inner>>,
}

impl Database {
    pub fn new(desc: DatabaseDesc) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new(desc))),
        }
    }

    pub async fn desc(&self) -> DatabaseDesc {
        self.inner.lock().await.desc.clone()
    }

    pub async fn collection(&self, id: u64) -> Result<CollectionDesc> {
        self.inner.lock().await.collection(id)
    }

    pub async fn lookup_collection(&self, name: &str) -> Result<CollectionDesc> {
        self.inner.lock().await.lookup_collection(name)
    }

    pub async fn create_collection(&self, spec: CollectionSpec) -> Result<CollectionDesc> {
        self.inner.lock().await.create_collection(spec)
    }
}

struct Inner {
    desc: DatabaseDesc,
    next_id: u64,
    collections: HashMap<u64, CollectionDesc>,
    collection_names: HashMap<String, u64>,
}

impl Inner {
    fn new(desc: DatabaseDesc) -> Self {
        Self {
            desc,
            next_id: 1,
            collections: HashMap::new(),
            collection_names: HashMap::new(),
        }
    }

    fn collection(&self, id: u64) -> Result<CollectionDesc> {
        self.collections
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("collection id {}", id)))
    }

    fn lookup_collection(&self, name: &str) -> Result<CollectionDesc> {
        let id = self
            .collection_names
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("collection name {}", name)))?;
        self.collection(id)
    }

    fn create_collection(&mut self, spec: CollectionSpec) -> Result<CollectionDesc> {
        if self.collection_names.contains_key(&spec.name) {
            return Err(Error::AlreadyExists(format!(
                "collection name {}",
                spec.name
            )));
        }
        let id = self.next_id;
        self.next_id += 1;
        let name = spec.name.clone();
        let desc = CollectionDesc {
            id,
            parent_id: self.desc.id,
            spec: Some(spec),
        };
        self.collections.insert(id, desc.clone());
        self.collection_names.insert(name, id);
        Ok(desc)
    }
}
