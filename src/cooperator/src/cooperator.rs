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

use crate::{Executor, Result};

#[derive(Clone)]
pub struct Cooperator {
    inner: Arc<Mutex<Universe>>,
}

impl Default for Cooperator {
    fn default() -> Self {
        Self::new()
    }
}

impl Cooperator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Universe::new())),
        }
    }

    pub async fn execute(&self, req: TxnRequest) -> Result<TxnResponse> {
        self.inner.lock().await.execute(req)
    }
}

struct Universe {
    databases: HashMap<String, Database>,
}

impl Universe {
    fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }

    fn execute(&mut self, req: TxnRequest) -> Result<TxnResponse> {
        let mut res = TxnResponse::default();
        for dbreq in req.requests {
            // Assumes that all databases exist for now.
            let db = self
                .databases
                .entry(dbreq.name.clone())
                .or_insert_with(Database::new);
            let dbres = db.execute(dbreq)?;
            res.responses.push(dbres);
        }
        Ok(res)
    }
}

struct Database {
    collections: HashMap<String, Collection>,
}

impl Database {
    fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    fn execute(&mut self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let mut res = DatabaseTxnResponse::default();
        for coreq in req.requests {
            // Assumes that all collections exist for now.
            let co = self
                .collections
                .entry(coreq.name.clone())
                .or_insert_with(Collection::new);
            let cores = co.execute(coreq)?;
            res.responses.push(cores);
        }
        Ok(res)
    }
}

struct Collection {
    exec: Executor,
}

impl Collection {
    fn new() -> Self {
        Self {
            exec: Executor::new(),
        }
    }

    fn execute(&mut self, req: CollectionTxnRequest) -> Result<CollectionTxnResponse> {
        let mut res = CollectionTxnResponse::default();
        for expr in req.exprs {
            let result = self.exec.execute(expr)?;
            res.results.push(result);
        }
        Ok(res)
    }
}
