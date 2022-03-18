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

use crate::{Database, Error, Result};

#[derive(Clone)]
pub struct Universe {
    inner: Arc<Mutex<UniverseInner>>,
}

impl Universe {
    pub fn new(sv: Supervisor) -> Self {
        let inner = UniverseInner::new(sv);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn database(&self, name: &str) -> Result<Database> {
        let mut inner = self.inner.lock().await;
        inner.database(name).await
    }
}

struct UniverseInner {
    sv: Supervisor,
    databases: HashMap<u64, Database>,
}

impl UniverseInner {
    fn new(sv: Supervisor) -> Self {
        Self {
            sv,
            databases: HashMap::new(),
        }
    }

    async fn database(&mut self, name: &str) -> Result<Database> {
        let req = DescribeDatabaseRequest {
            name: name.to_owned(),
        };
        let res = self.sv.describe_database(req).await?;
        let desc = res
            .desc
            .ok_or_else(|| Error::internal("missing database descriptor"))?;
        let db = self
            .databases
            .entry(desc.id)
            .or_insert_with(|| Database::new(desc, self.sv.clone()));
        Ok(db.clone())
    }
}
