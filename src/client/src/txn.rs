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

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use engula_apis::*;

use crate::{expr::simple, txn_client::TxnClient, Error, Object, Result};

#[derive(Clone)]
pub struct DatabaseTxn {
    inner: Arc<DatabaseTxnInner>,
}

struct DatabaseTxnInner {
    handle: DatabaseTxnHandle,
    requests: Mutex<Vec<CollectionTxnRequest>>,
}

struct DatabaseTxnHandle {
    dbname: String,
    client: TxnClient,
}

impl DatabaseTxn {
    pub(crate) fn new(dbname: String, client: TxnClient) -> Self {
        let inner = DatabaseTxnInner {
            handle: DatabaseTxnHandle { dbname, client },
            requests: Mutex::new(Vec::new()),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn collection<T: Object>(&self, coname: String) -> CollectionTxn<T> {
        CollectionTxn::new_with(coname, self.inner.clone())
    }

    pub async fn commit(self) -> Result<()> {
        let inner = Arc::try_unwrap(self.inner)
            .map_err(|_| Error::InvalidOperation("there are pending transactions".to_owned()))?;
        let mut handle = inner.handle;
        let req = DatabaseTxnRequest {
            name: handle.dbname,
            requests: inner.requests.into_inner().unwrap(),
        };
        handle.client.database(req).await?;
        Ok(())
    }
}

pub struct CollectionTxn<T> {
    inner: Arc<CollectionTxnInner>,
    _marker: PhantomData<T>,
}

struct CollectionTxnInner {
    coname: String,
    handle: Option<DatabaseTxnHandle>,
    parent: Option<Arc<DatabaseTxnInner>>,
    exprs: Mutex<Vec<Expr>>,
}

struct CollectionTxnHandle {
    dbname: String,
    coname: String,
    client: TxnClient,
}

impl<T: Object> CollectionTxn<T> {
    pub(crate) fn new(dbname: String, coname: String, client: TxnClient) -> Self {
        let handle = DatabaseTxnHandle { dbname, client };
        Self::new_inner(coname, Some(handle), None)
    }

    fn new_with(coname: String, parent: Arc<DatabaseTxnInner>) -> Self {
        Self::new_inner(coname, None, Some(parent))
    }

    fn new_inner(
        coname: String,
        handle: Option<DatabaseTxnHandle>,
        parent: Option<Arc<DatabaseTxnInner>>,
    ) -> Self {
        let inner = CollectionTxnInner {
            coname,
            handle,
            parent,
            exprs: Mutex::new(Vec::new()),
        };
        Self {
            inner: Arc::new(inner),
            _marker: PhantomData,
        }
    }

    pub(crate) fn add_expr(&mut self, expr: Expr) {
        self.inner.exprs.lock().unwrap().push(expr);
    }

    pub fn set(&mut self, id: impl Into<Vec<u8>>, value: impl Into<Value>) {
        self.add_expr(simple::set(id, value.into()))
    }

    pub fn remove(&mut self, id: impl Into<Vec<u8>>) {
        self.add_expr(simple::remove(id))
    }

    pub fn object(&mut self, _id: impl Into<Vec<u8>>) {
        todo!();
    }

    pub async fn commit(self) -> Result<()> {
        let inner = Arc::try_unwrap(self.inner)
            .map_err(|_| Error::InvalidOperation("there are pending transactions".to_owned()))?;
        let req = CollectionTxnRequest {
            name: inner.coname,
            exprs: inner.exprs.into_inner().unwrap(),
        };
        if let Some(mut handle) = inner.handle {
            handle.client.collection(handle.dbname, req).await?;
        } else {
            let parent = inner.parent.unwrap();
            parent.requests.lock().unwrap().push(req);
        }
        Ok(())
    }
}

pub struct ObjectTxn {
    handle: Option<CollectionTxnHandle>,
    parent: Option<Arc<CollectionTxnInner>>,
    expr: Expr,
}

#[allow(dead_code)]
impl ObjectTxn {
    pub(crate) fn new(id: Vec<u8>, dbname: String, coname: String, client: TxnClient) -> Self {
        let handle = CollectionTxnHandle {
            dbname,
            coname,
            client,
        };
        Self::new_inner(id, Some(handle), None)
    }

    fn new_with(id: Vec<u8>, parent: Arc<CollectionTxnInner>) -> Self {
        Self::new_inner(id, None, Some(parent))
    }

    fn new_inner(
        id: Vec<u8>,
        handle: Option<CollectionTxnHandle>,
        parent: Option<Arc<CollectionTxnInner>>,
    ) -> Self {
        Self {
            handle,
            parent,
            expr: Expr {
                id,
                ..Default::default()
            },
        }
    }

    pub async fn commit(mut self) -> Result<()> {
        if let Some(mut handle) = self.handle.take() {
            let expr = std::mem::take(&mut self.expr);
            handle
                .client
                .collection_expr(handle.dbname, handle.coname, expr)
                .await?;
        }
        Ok(())
    }
}

impl Drop for ObjectTxn {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.take() {
            let expr = std::mem::take(&mut self.expr);
            parent.exprs.lock().unwrap().push(expr);
        }
    }
}
