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

use std::sync::Arc;

use engula_apis::*;

use crate::{
    expr::{simple, Value},
    txn_client::TxnClient,
    universe_client::UniverseClient,
    CollectionTxn, Error, Object, Result,
};

#[derive(Clone)]
pub struct Collection {
    inner: Arc<CollectionInner>,
}

impl Collection {
    pub(crate) fn new(
        dbname: String,
        coname: String,
        txn_client: TxnClient,
        universe_client: UniverseClient,
    ) -> Self {
        let inner = CollectionInner {
            dbname,
            coname,
            txn_client,
            universe_client,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn desc(&self) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest {
            name: self.inner.coname.clone(),
        };
        let req = collection_request_union::Request::DescribeCollection(req);
        let res = self.inner.collection_union_call(req).await?;
        if let collection_response_union::Response::DescribeCollection(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub fn begin(&self) -> CollectionTxn {
        self.inner.new_txn()
    }

    pub fn object(&self, id: impl Into<Vec<u8>>) -> Object {
        self.inner.new_object(id.into())
    }

    pub async fn get(&self, id: impl Into<Vec<u8>>) -> Result<Value> {
        let expr = simple::get(id);
        let result = self.inner.collection_expr_call(expr).await?;
        result.value.map(|v| v.into()).ok_or(Error::InvalidResponse)
    }

    pub async fn set(&self, id: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        let mut txn = self.begin();
        txn.set(id, value);
        txn.commit().await
    }

    pub async fn delete(&self, id: impl Into<Vec<u8>>) -> Result<()> {
        let mut txn = self.begin();
        txn.delete(id);
        txn.commit().await
    }
}

pub struct CollectionInner {
    dbname: String,
    coname: String,
    txn_client: TxnClient,
    universe_client: UniverseClient,
}

impl CollectionInner {
    fn new_txn(&self) -> CollectionTxn {
        CollectionTxn::new(
            self.dbname.clone(),
            self.coname.clone(),
            self.txn_client.clone(),
        )
    }

    fn new_object(&self, id: Vec<u8>) -> Object {
        Object::new(
            id,
            self.dbname.clone(),
            self.coname.clone(),
            self.txn_client.clone(),
        )
    }

    async fn collection_expr_call(&self, expr: Expr) -> Result<ExprResult> {
        self.txn_client
            .clone()
            .collection_expr(self.dbname.clone(), self.coname.clone(), expr)
            .await
    }

    async fn collection_union_call(
        &self,
        req: collection_request_union::Request,
    ) -> Result<collection_response_union::Response> {
        self.universe_client
            .clone()
            .collection_union(self.dbname.clone(), req)
            .await
    }
}
