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

use crate::{database::Database, Error, Result};

#[derive(Clone)]
pub struct Universe {
    inner: Arc<Mutex<Inner>>,
}

impl Universe {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    pub async fn database(&self, id: u64) -> Result<Database> {
        self.inner.lock().await.database(id)
    }

    pub async fn lookup_database(&self, name: &str) -> Result<Database> {
        self.inner.lock().await.lookup_database(name)
    }

    pub async fn create_database(&self, spec: DatabaseSpec) -> Result<DatabaseDesc> {
        self.inner.lock().await.create_database(spec)
    }
}

struct Inner {
    next_id: u64,
    databases: HashMap<u64, Database>,
    database_names: HashMap<String, u64>,
}

impl Inner {
    fn new() -> Self {
        Self {
            next_id: 1,
            databases: HashMap::new(),
            database_names: HashMap::new(),
        }
    }

    fn database(&self, id: u64) -> Result<Database> {
        self.databases
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("database id {}", id)))
    }

    fn lookup_database(&self, name: &str) -> Result<Database> {
        let id = self
            .database_names
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("database name {}", name)))?;
        self.database(id)
    }

    fn create_database(&mut self, spec: DatabaseSpec) -> Result<DatabaseDesc> {
        if self.database_names.contains_key(&spec.name) {
            return Err(Error::AlreadyExists(format!("database name {}", spec.name)));
        }
        let id = self.next_id;
        self.next_id += 1;
        let name = spec.name.clone();
        let desc = DatabaseDesc {
            id,
            spec: Some(spec),
        };
        let db = Database::new(desc.clone());
        self.databases.insert(id, db);
        self.database_names.insert(name, id);
        Ok(desc)
    }
}
