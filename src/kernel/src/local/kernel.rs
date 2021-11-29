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

use engula_journal::MemJournal;
use engula_storage::MemStorage;
use tokio::sync::Mutex;

use super::LocalEngine;
use crate::{async_trait, Engine, Error, Kernel, Result};

pub struct LocalKernel {
    inner: Mutex<Inner>,
}

struct Inner {
    engines: HashMap<String, LocalEngine>,
}

#[async_trait]
impl Kernel for LocalKernel {
    async fn engine(&self, name: &str) -> Result<Box<dyn Engine>> {
        let inner = self.inner.lock().await;
        match inner.engines.get(name) {
            Some(engine) => Ok(Box::new(engine.clone())),
            None => Err(Error::NotFound(format!("engine '{}'", name))),
        }
    }

    async fn create_engine(&self, name: &str) -> Result<Box<dyn Engine>> {
        let journal = MemJournal::default();
        let storage = MemStorage::default();
        let engine = LocalEngine::new(Box::new(journal), Box::new(storage));
        let mut inner = self.inner.lock().await;
        match inner.engines.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(engine.clone());
                Ok(Box::new(engine))
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("engine '{}'", ent.key())))
            }
        }
    }
}
