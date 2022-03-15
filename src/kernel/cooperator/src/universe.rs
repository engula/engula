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

use std::{collections::BTreeMap, sync::Arc};

use engula_apis::*;
use engula_supervisor::Supervisor;
use tokio::sync::Mutex;

use crate::{Database, Result};

#[derive(Clone)]
pub struct Universe {
    inner: Arc<Mutex<Inner>>,
}

impl Universe {
    pub fn new(supervisor: Supervisor) -> Self {
        let inner = Inner::new(supervisor);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn execute(&self, req: TxnRequest) -> Result<TxnResponse> {
        let mut inner = self.inner.lock().await;
        let mut res = TxnResponse::default();
        for dbreq in req.requests {
            let db = inner.database(&dbreq.name).await?;
            let dbres = db.execute(dbreq).await?;
            res.responses.push(dbres);
        }
        Ok(res)
    }
}

struct Inner {
    sp: Supervisor,
    databases: BTreeMap<u64, Database>,
}

impl Inner {
    fn new(supervisor: Supervisor) -> Self {
        Self {
            sp: supervisor,
            databases: BTreeMap::new(),
        }
    }

    async fn database(&mut self, name: &str) -> Result<Database> {
        let desc = self.sp.describe_database(name.to_owned()).await?;
        let db = self
            .databases
            .entry(desc.id)
            .or_insert_with(|| Database::new(desc, self.sp.clone()))
            .clone();
        Ok(db)
    }
}
