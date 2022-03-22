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

use engula_apis::v1::*;
use engula_supervisor::Supervisor;
use tokio::sync::Mutex;

use crate::{Collection, Error, Result, WriteBatch};

#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

impl Database {
    pub fn new(desc: DatabaseDesc, sv: Supervisor) -> Self {
        let inner = DatabaseInner::new(desc, sv);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn execute(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let mut inner = self.inner.lock().await;
        let mut res = DatabaseResponse::default();
        let mut cx = DatabaseContext::default();
        for coreq in req.requests {
            let co = inner.collection(&coreq.name).await?;
            let mut wb = WriteBatch::default();
            let cores = co.execute(&mut wb, coreq).await?;
            res.responses.push(cores);
            cx.collections.push((co, wb));
        }
        for (co, wb) in cx.collections {
            co.write(wb).await;
        }
        Ok(res)
    }
}

struct DatabaseInner {
    sv: Supervisor,
    desc: DatabaseDesc,
    collections: HashMap<u64, Collection>,
}

impl DatabaseInner {
    fn new(desc: DatabaseDesc, sv: Supervisor) -> Self {
        Self {
            sv,
            desc,
            collections: HashMap::new(),
        }
    }

    async fn collection(&mut self, name: &str) -> Result<Collection> {
        let req = DescribeCollectionRequest {
            name: name.to_owned(),
            dbname: self.desc.name.clone(),
        };
        let res = self.sv.describe_collection(req).await?;
        let desc = res
            .desc
            .ok_or_else(|| Error::internal("missing collection descriptor"))?;
        let co = self
            .collections
            .entry(desc.id)
            .or_insert_with(|| Collection::new(desc));
        Ok(co.clone())
    }
}

#[derive(Default)]
struct DatabaseContext {
    collections: Vec<(Collection, WriteBatch)>,
}
