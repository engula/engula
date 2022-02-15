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

use std::{marker::PhantomData, sync::Arc};

use engula_apis::*;

use crate::{
    expr::simple, txn_client::TxnClient, universe_client::UniverseClient, Any, CollectionTxn,
    DatabaseTxn, Error, Object, ObjectValue, Result,
};

#[derive(Clone)]
pub struct Collection<T> {
    inner: Arc<CollectionInner>,
    _marker: PhantomData<T>,
}

impl<T: Object> Collection<T> {
    pub(crate) fn new(
        name: String,
        dbname: String,
        txn_client: TxnClient,
        universe_client: UniverseClient,
    ) -> Self {
        let inner = CollectionInner {
            dbname,
            coname: name,
            txn_client,
            universe_client,
        };
        Self {
            inner: Arc::new(inner),
            _marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.coname
    }

    pub async fn desc(&self) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest {
            name: self.inner.coname.clone(),
        };
        let req = collection_request_union::Request::DescribeCollection(req);
        let res = self.inner.collection_union_call(req).await?;
        let desc = if let collection_response_union::Response::DescribeCollection(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or(Error::InvalidResponse)
    }

    pub fn begin(&self) -> CollectionTxn<T> {
        self.inner.new_txn()
    }

    pub fn begin_with(&self, parent: DatabaseTxn) -> CollectionTxn<T> {
        parent.collection(self.inner.coname.clone())
    }

    pub fn object(&self, id: impl Into<Vec<u8>>) -> T {
        self.inner.new_object(id.into())
    }

    pub async fn get(&self, id: impl Into<Vec<u8>>) -> Result<Option<T::Value>> {
        let expr = simple::get(id);
        let result = self.inner.collection_expr_call(expr).await?;
        T::Value::cast_from_option(result.value)
    }

    pub async fn set(&self, id: impl Into<Vec<u8>>, value: impl Into<T::Value>) -> Result<()> {
        let expr = simple::set(id, value.into());
        self.inner.collection_expr_call(expr).await?;
        Ok(())
    }

    pub async fn remove(&self, id: impl Into<Vec<u8>>) -> Result<()> {
        let expr = simple::remove(id);
        self.inner.collection_expr_call(expr).await?;
        Ok(())
    }
}

pub struct CollectionInner {
    dbname: String,
    coname: String,
    txn_client: TxnClient,
    universe_client: UniverseClient,
}

impl CollectionInner {
    fn new_txn<T: Object>(&self) -> CollectionTxn<T> {
        CollectionTxn::new(
            self.dbname.clone(),
            self.coname.clone(),
            self.txn_client.clone(),
        )
    }

    fn new_object<T: Object>(&self, id: Vec<u8>) -> T {
        Any::new(
            id,
            self.dbname.clone(),
            self.coname.clone(),
            self.txn_client.clone(),
        )
        .into()
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
