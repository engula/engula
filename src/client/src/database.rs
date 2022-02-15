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
    txn_client::TxnClient, universe_client::UniverseClient, Collection, DatabaseTxn, Error, Object,
    Result,
};

#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    pub fn new(name: String, txn_client: TxnClient, universe_client: UniverseClient) -> Self {
        let inner = DatabaseInner {
            name,
            txn_client,
            universe_client,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn desc(&self) -> Result<DatabaseDesc> {
        let req = DescribeDatabaseRequest {
            name: self.inner.name.clone(),
        };
        let req = database_request_union::Request::DescribeDatabase(req);
        let res = self.inner.database_union_call(req).await?;
        let desc = if let database_response_union::Response::DescribeDatabase(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or(Error::InvalidResponse)
    }

    pub fn begin(&self) -> DatabaseTxn {
        self.inner.new_txn()
    }

    pub fn collection<T: Object>(&self, name: impl Into<String>) -> Collection<T> {
        self.inner.new_collection(name.into())
    }

    pub async fn create_collection<T: Object>(
        &self,
        name: impl Into<String>,
    ) -> Result<Collection<T>> {
        let name = name.into();
        let spec = CollectionSpec { name: name.clone() };
        let req = CreateCollectionRequest { spec: Some(spec) };
        let req = collection_request_union::Request::CreateCollection(req);
        self.inner.collection_union_call(req).await?;
        Ok(self.collection(name))
    }

    pub async fn delete_collection(&self, name: impl Into<String>) -> Result<()> {
        let req = DeleteCollectionRequest { name: name.into() };
        let req = collection_request_union::Request::DeleteCollection(req);
        self.inner.collection_union_call(req).await?;
        Ok(())
    }
}

struct DatabaseInner {
    name: String,
    txn_client: TxnClient,
    universe_client: UniverseClient,
}

impl DatabaseInner {
    fn new_txn(&self) -> DatabaseTxn {
        DatabaseTxn::new(self.name.clone(), self.txn_client.clone())
    }

    fn new_collection<T: Object>(&self, name: String) -> Collection<T> {
        Collection::new(
            self.name.clone(),
            name,
            self.txn_client.clone(),
            self.universe_client.clone(),
        )
    }

    async fn database_union_call(
        &self,
        req: database_request_union::Request,
    ) -> Result<database_response_union::Response> {
        self.universe_client.clone().database_union(req).await
    }

    async fn collection_union_call(
        &self,
        req: collection_request_union::Request,
    ) -> Result<collection_response_union::Response> {
        self.universe_client
            .clone()
            .collection_union(self.name.clone(), req)
            .await
    }
}
