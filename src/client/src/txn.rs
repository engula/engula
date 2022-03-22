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

use crate::{Any, Client, Error, Result};

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

    pub async fn commit(self) -> Result<()> {
        let inner =
            Arc::try_unwrap(self.inner).map_err(|_| Error::aborted("pending transactions"))?;
        let req = DatabaseRequest {
            name: inner.name,
            requests: inner.requests.into_inner().unwrap(),
        };
        inner.client.database(req).await?;
        Ok(())
    }
}

struct DatabaseInner {
    name: String,
    client: Client,
    requests: Mutex<Vec<CollectionRequest>>,
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
            requests: Mutex::new(Vec::new()),
        }
    }

    fn add_request(&self, req: CollectionRequest) {
        self.requests.lock().unwrap().push(req);
    }
}

pub struct CollectionTxn {
    handle: Option<DatabaseHandle>,
    parent: Option<Arc<DatabaseInner>>,
    request: CollectionRequest,
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
            request: CollectionRequest {
                name,
                ..Default::default()
            },
        }
    }

    pub fn set(&mut self, id: impl Into<Vec<u8>>, value: impl Into<Value>) {
        self.mutate(id, Any::set(value));
    }

    pub fn delete(&mut self, id: impl Into<Vec<u8>>) {
        self.mutate(id, Any::delete());
    }

    pub fn mutate(&mut self, id: impl Into<Vec<u8>>, mutate: impl Into<MutateExpr>) {
        let expr = ObjectExpr {
            batch: vec![id.into()],
            mutate: Some(mutate.into()),
            ..Default::default()
        };
        self.request.exprs.push(expr);
    }

    pub fn submit(self) {
        let parent = self.parent.unwrap();
        parent.add_request(self.request);
    }

    pub async fn commit(self) -> Result<()> {
        let handle = self.handle.unwrap();
        let req = DatabaseRequest {
            name: handle.name,
            requests: vec![self.request],
        };
        handle.client.database(req).await?;
        Ok(())
    }
}
