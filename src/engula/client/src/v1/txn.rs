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

use std::sync::{Arc, Mutex};

use engula_apis::v1::*;

use super::{Client, Result};

#[derive(Clone)]
pub struct DatabaseTxn {
    inner: Arc<DatabaseInner>,
}

impl DatabaseTxn {
    pub(crate) fn new(name: String, client: Client) -> Self {
        let inner = DatabaseInner::new(name, client);
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn collection(&self, name: &str) -> CollectionTxn {
        CollectionTxn::new_with(name.to_owned(), self.inner.clone())
    }
}

struct DatabaseInner {
    name: String,
    client: Client,
    mutates: Mutex<Vec<MutateCollectionRequest>>,
}

struct DatabaseHandle {
    name: String,
    client: Client,
}

impl DatabaseInner {
    fn new(name: String, client: Client) -> Self {
        Self {
            name,
            client,
            mutates: Mutex::new(Vec::new()),
        }
    }

    fn add_mutate(&self, mutate: MutateCollectionRequest) {
        let mut mutates = self.mutates.lock().unwrap();
        mutates.push(mutate);
    }
}

pub struct CollectionTxn {
    handle: Option<DatabaseHandle>,
    parent: Option<Arc<DatabaseInner>>,
    mutate: MutateCollectionRequest,
}

impl CollectionTxn {
    pub(crate) fn new(name: String, dbname: String, client: Client) -> Self {
        let handle = DatabaseHandle {
            name: dbname,
            client,
        };
        Self::new_inner(name, Some(handle), None)
    }

    fn new_with(name: String, parent: Arc<DatabaseInner>) -> Self {
        Self::new_inner(name, None, Some(parent))
    }

    fn new_inner(
        name: String,
        handle: Option<DatabaseHandle>,
        parent: Option<Arc<DatabaseInner>>,
    ) -> Self {
        Self {
            handle,
            parent,
            mutate: MutateCollectionRequest {
                name,
                ..Default::default()
            },
        }
    }

    pub fn mutate(&mut self, id: impl Into<Vec<u8>>, expr: impl Into<MutateExpr>) -> &mut Self {
        self.mutate.ids.push(id.into());
        self.mutate.exprs.push(expr.into());
        self
    }

    pub fn submit(self) {
        let parent = self.parent.unwrap();
        parent.add_mutate(self.mutate);
    }

    pub async fn commit(self) -> Result<()> {
        let handle = self.handle.unwrap();
        let req = DatabaseTxnRequest {
            name: handle.name,
            mutates: vec![self.mutate],
            ..Default::default()
        };
        handle.client.txn(req).await?;
        Ok(())
    }
}
