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

use crate::{Collection, Result};

#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<Inner>>,
}

impl Database {
    pub fn new(desc: DatabaseDesc, supervisor: Supervisor) -> Self {
        let inner = Inner::new(desc, supervisor);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn execute(&self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let mut inner = self.inner.lock().await;
        let mut res = DatabaseTxnResponse::default();
        for coreq in req.requests {
            let co = inner.collection(&coreq.name).await?;
            let cores = co.execute(coreq).await?;
            res.responses.push(cores);
        }
        Ok(res)
    }
}

struct Inner {
    sp: Supervisor,
    desc: DatabaseDesc,
    collections: BTreeMap<u64, Collection>,
}

impl Inner {
    fn new(desc: DatabaseDesc, supervisor: Supervisor) -> Self {
        Self {
            sp: supervisor,
            desc,
            collections: BTreeMap::new(),
        }
    }

    async fn collection(&mut self, name: &str) -> Result<Collection> {
        let desc = self
            .sp
            .describe_collection(self.desc.name.clone(), name.to_owned())
            .await?;
        let co = self
            .collections
            .entry(desc.id)
            .or_insert_with(Collection::new)
            .clone();
        Ok(co)
    }
}
