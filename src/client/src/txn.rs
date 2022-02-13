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

use engula_apis::*;

use crate::{
    expr::{simple, Value},
    txn_client::TxnClient,
    Error, Result,
};

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

    fn add_request(&self, req: CollectionTxnRequest) {
        self.inner.requests.lock().unwrap().push(req);
    }

    pub fn collection(&self, coname: impl Into<String>) -> CollectionTxn {
        CollectionTxn::new_with(coname.into(), self.clone())
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

pub struct CollectionTxn {
    coname: String,
    handle: Option<DatabaseTxnHandle>,
    parent: Option<DatabaseTxn>,
    exprs: Vec<Expr>,
}

struct CollectionTxnHandle {
    dbname: String,
    coname: String,
    client: TxnClient,
}

impl CollectionTxn {
    pub(crate) fn new(dbname: String, coname: String, client: TxnClient) -> Self {
        let handle = DatabaseTxnHandle { dbname, client };
        Self {
            coname,
            handle: Some(handle),
            parent: None,
            exprs: Vec::new(),
        }
    }

    pub(crate) fn new_with(coname: String, parent: DatabaseTxn) -> Self {
        Self {
            coname,
            handle: None,
            parent: Some(parent),
            exprs: Vec::new(),
        }
    }

    fn add_expr(&mut self, expr: Expr) {
        self.exprs.push(expr);
    }

    pub fn set(&mut self, id: impl Into<Vec<u8>>, value: impl Into<Value>) {
        self.add_expr(simple::set(id, value.into()))
    }

    pub fn delete(&mut self, id: impl Into<Vec<u8>>) {
        self.add_expr(simple::delete(id))
    }

    pub fn object(&mut self, id: impl Into<Vec<u8>>) -> ObjectTxn {
        ObjectTxn::new_with(id.into(), self)
    }

    pub async fn commit(self) -> Result<()> {
        let req = CollectionTxnRequest {
            name: self.coname,
            exprs: self.exprs,
        };
        if let Some(mut handle) = self.handle {
            handle.client.collection(handle.dbname, req).await?;
        } else {
            self.parent.unwrap().add_request(req);
        }
        Ok(())
    }
}

pub struct ObjectTxn<'a> {
    handle: Option<CollectionTxnHandle>,
    parent: Option<&'a mut CollectionTxn>,
    _id: Vec<u8>,
    expr: Expr,
}

impl<'a> ObjectTxn<'a> {
    pub(crate) fn _new(id: Vec<u8>, dbname: String, coname: String, client: TxnClient) -> Self {
        let handle = CollectionTxnHandle {
            dbname,
            coname,
            client,
        };
        Self::new_inner(id, Some(handle), None)
    }

    pub(crate) fn new_with(id: Vec<u8>, parent: &'a mut CollectionTxn) -> Self {
        Self::new_inner(id, None, Some(parent))
    }

    fn new_inner(
        id: Vec<u8>,
        handle: Option<CollectionTxnHandle>,
        parent: Option<&'a mut CollectionTxn>,
    ) -> Self {
        Self {
            handle,
            parent,
            _id: id,
            expr: Expr::default(),
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

impl<'a> Drop for ObjectTxn<'a> {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.take() {
            parent.add_expr(std::mem::take(&mut self.expr));
        }
    }
}
