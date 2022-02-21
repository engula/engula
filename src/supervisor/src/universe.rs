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

use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Universe {
    inner: Arc<Mutex<UniverseInner>>,
}

struct UniverseInner {
    next_id: u64,
    databases: HashMap<String, Database>,
}

impl Universe {
    pub fn new() -> Self {
        let inner = UniverseInner {
            next_id: 1,
            databases: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn database(&self, name: &str) -> Result<Database> {
        let inner = self.inner.lock().await;
        inner
            .databases
            .get(name)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("database {}", name)))
    }

    pub async fn create_database(&self, mut desc: DatabaseDesc) -> Result<DatabaseDesc> {
        let mut inner = self.inner.lock().await;
        if inner.databases.contains_key(&desc.name) {
            return Err(Error::already_exists(format!("database {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        let db = Database::new(desc.clone());
        inner.databases.insert(desc.name.clone(), db);
        Ok(desc)
    }
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

struct DatabaseInner {
    desc: DatabaseDesc,
    next_id: u64,
    collections: HashMap<String, Collection>,
}

impl Database {
    fn new(desc: DatabaseDesc) -> Self {
        let inner = DatabaseInner {
            desc,
            next_id: 1,
            collections: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> DatabaseDesc {
        self.inner.lock().await.desc.clone()
    }

    pub async fn collection(&self, name: &str) -> Result<Collection> {
        let inner = self.inner.lock().await;
        inner
            .collections
            .get(name)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("collection {}", name)))
    }

    pub async fn create_collection(&self, mut desc: CollectionDesc) -> Result<CollectionDesc> {
        let mut inner = self.inner.lock().await;
        if inner.collections.contains_key(&desc.name) {
            return Err(Error::already_exists(format!("collection {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        desc.parent_id = inner.desc.id;
        let co = Collection::new(desc.clone());
        inner.collections.insert(desc.name.clone(), co);
        Ok(desc)
    }
}

#[derive(Clone)]
pub struct Collection {
    inner: Arc<Mutex<CollectionInner>>,
}

struct CollectionInner {
    desc: CollectionDesc,
}

impl Collection {
    fn new(desc: CollectionDesc) -> Self {
        let inner = CollectionInner { desc };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> CollectionDesc {
        self.inner.lock().await.desc.clone()
    }
}
