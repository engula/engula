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

use engula_apis::*;

use crate::{
    txn_client::TxnClient, universe_client::UniverseClient, DatabaseTxn, Error, Object, ObjectTxn,
    Result,
};

pub struct Collection {
    desc: CollectionDesc,
    txn_client: TxnClient,
    universe_client: UniverseClient,
}

impl Collection {
    pub fn new(
        desc: CollectionDesc,
        txn_client: TxnClient,
        universe_client: UniverseClient,
    ) -> Self {
        Self {
            desc,
            txn_client,
            universe_client,
        }
    }

    pub async fn desc(&self) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest {
            id: self.desc.id,
            ..Default::default()
        };
        let req = collections_request_union::Request::DescribeCollection(req);
        let res = self
            .universe_client
            .clone()
            .collections_union(self.desc.database_id, req)
            .await?;
        if let collections_response_union::Response::DescribeCollection(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub fn begin(&self) -> CollectionTxn {
        CollectionTxn::new(
            None,
            self.txn_client.clone(),
            self.desc.database_id,
            self.desc.id,
        )
    }

    pub fn begin_with(&self, txn: DatabaseTxn) -> CollectionTxn {
        CollectionTxn::new(
            Some(txn),
            self.txn_client.clone(),
            self.desc.database_id,
            self.desc.id,
        )
    }

    pub fn object(&self, object_id: impl Into<Vec<u8>>) -> Object {
        Object {
            client: self.txn_client.clone(),
            object_id: object_id.into(),
            database_id: self.desc.database_id,
            collection_id: self.desc.id,
        }
    }
}

pub struct CollectionTxn {
    parent: Option<DatabaseTxn>,
    client: TxnClient,
    database_id: u64,
    collection_id: u64,
    exprs: Vec<MethodCallExpr>,
}

impl CollectionTxn {
    fn new(
        parent: Option<DatabaseTxn>,
        client: TxnClient,
        database_id: u64,
        collection_id: u64,
    ) -> Self {
        Self {
            parent,
            client,
            database_id,
            collection_id,
            exprs: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, expr: MethodCallExpr) {
        self.exprs.push(expr);
    }

    pub fn object(&mut self, object_id: impl Into<Vec<u8>>) -> ObjectTxn {
        ObjectTxn::new(self, object_id)
    }

    pub async fn commit(mut self) -> Result<()> {
        let req = CollectionTxnRequest {
            collection_id: self.collection_id,
            exprs: self.exprs,
        };
        if let Some(parent) = self.parent {
            parent.add(req).await;
        } else {
            self.client.collection_call(self.database_id, req).await?;
        }
        Ok(())
    }
}
