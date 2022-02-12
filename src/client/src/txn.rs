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
    expr::{call, Value},
    txn_client::TxnClient,
    Error, Result,
};

#[derive(Clone)]
pub struct DatabaseTxn {
    inner: Arc<Mutex<DatabaseTxnInner>>,
}

struct DatabaseTxnInner {
    name: String,
    client: TxnClient,
    collections: Vec<CollectionTxnRequest>,
}

impl DatabaseTxn {
    pub(crate) fn new(name: String, client: TxnClient) -> Self {
        let inner = DatabaseTxnInner {
            name,
            client,
            collections: Vec::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub(crate) fn add_collection(&self, co: CollectionTxnRequest) {
        let mut inner = self.inner.lock().unwrap();
        inner.collections.push(co);
    }

    pub fn collection(&self, name: impl Into<String>) -> CollectionTxn {
        CollectionTxn::new_borrowed(name.into(), self.clone())
    }

    pub async fn commit(self) -> Result<()> {
        let mut inner = Arc::try_unwrap(self.inner)
            .map(|x| x.into_inner().unwrap())
            .map_err(|_| Error::InvalidOperation("there are pending transactions".to_owned()))?;
        inner
            .client
            .collections(inner.name, inner.collections)
            .await?;
        Ok(())
    }
}

struct DatabaseTxnHandle {
    dbname: String,
    client: TxnClient,
}

#[derive(Clone)]
pub struct CollectionTxn {
    inner: Arc<Mutex<CollectionTxnInner>>,
}

struct CollectionTxnInner {
    handle: Option<DatabaseTxnHandle>,
    parent: Option<DatabaseTxn>,
    request: CollectionTxnRequest,
}

impl CollectionTxn {
    fn new(coname: String, handle: Option<DatabaseTxnHandle>, parent: Option<DatabaseTxn>) -> Self {
        let request = CollectionTxnRequest {
            name: coname,
            ..Default::default()
        };
        let inner = CollectionTxnInner {
            handle,
            parent,
            request,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub(crate) fn new_owned(dbname: String, coname: String, client: TxnClient) -> Self {
        let handle = DatabaseTxnHandle { dbname, client };
        Self::new(coname, Some(handle), None)
    }

    pub(crate) fn new_borrowed(coname: String, parent: DatabaseTxn) -> Self {
        Self::new(coname, None, Some(parent))
    }

    pub(crate) fn add_method(&mut self, method: MethodCallExpr) {
        let mut inner = self.inner.lock().unwrap();
        inner.request.methods.push(method);
    }

    pub fn object(&mut self, id: impl Into<Vec<u8>>) -> ObjectTxn {
        ObjectTxn::new_borrowed(id.into(), self.clone())
    }

    pub async fn commit(self) -> Result<()> {
        let inner = Arc::try_unwrap(self.inner)
            .map(|x| x.into_inner().unwrap())
            .map_err(|_| Error::InvalidOperation("there are pending transactions".to_owned()))?;
        if let Some(mut handle) = inner.handle {
            handle
                .client
                .collection(handle.dbname, inner.request)
                .await?;
        } else {
            inner.parent.unwrap().add_collection(inner.request);
        }
        Ok(())
    }
}

struct CollectionTxnHandle {
    dbname: String,
    coname: String,
    client: TxnClient,
}

pub struct ObjectTxn {
    handle: Option<CollectionTxnHandle>,
    parent: Option<CollectionTxn>,
    method: MethodCallExpr,
}

impl ObjectTxn {
    fn new(
        id: Vec<u8>,
        handle: Option<CollectionTxnHandle>,
        parent: Option<CollectionTxn>,
    ) -> Self {
        let method = MethodCallExpr {
            index: Some(method_call_expr::Index::BlobIdent(id)),
            ..Default::default()
        };
        Self {
            handle,
            parent,
            method,
        }
    }

    pub(crate) fn new_owned(
        id: Vec<u8>,
        dbname: String,
        coname: String,
        client: TxnClient,
    ) -> Self {
        let handle = CollectionTxnHandle {
            dbname,
            coname,
            client,
        };
        Self::new(id, Some(handle), None)
    }

    pub(crate) fn new_borrowed(id: Vec<u8>, parent: CollectionTxn) -> Self {
        Self::new(id, None, Some(parent))
    }

    pub fn set(mut self, value: impl Into<Value>) -> Self {
        self.method.call = Some(call::set(value.into()));
        self
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, value: impl Into<Value>) -> Self {
        self.method.call = Some(call::add(value.into()));
        self
    }

    pub fn delete(mut self) -> Self {
        self.method.call = Some(call::delete());
        self
    }

    pub async fn commit(mut self) -> Result<()> {
        if let Some(mut handle) = self.handle.take() {
            let method = std::mem::take(&mut self.method);
            handle
                .client
                .method(handle.dbname, handle.coname, method)
                .await?;
        }
        Ok(())
    }
}

impl Drop for ObjectTxn {
    fn drop(&mut self) {
        if let Some(mut parent) = self.parent.take() {
            parent.add_method(std::mem::take(&mut self.method));
        }
    }
}
