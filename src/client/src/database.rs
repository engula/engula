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

use crate::{Client, Collection, DatabaseTxn, Error, Object, Result};

#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    pub fn new(name: String, client: Client) -> Self {
        let inner = DatabaseInner { name, client };
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
        desc.ok_or_else(|| Error::internal("missing database description"))
    }

    pub fn begin(&self) -> DatabaseTxn {
        self.inner.new_txn()
    }

    pub fn collection<T: Object>(&self, name: &str) -> Collection<T> {
        self.inner.new_collection(name.to_owned())
    }

    pub async fn create_collection<T: Object>(&self, name: &str) -> Result<Collection<T>> {
        let desc = CollectionDesc {
            name: name.to_owned(),
            ..Default::default()
        };
        let req = CreateCollectionRequest { desc: Some(desc) };
        let req = collection_request_union::Request::CreateCollection(req);
        self.inner.collection_union_call(req).await?;
        Ok(self.collection(name))
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        let req = DeleteCollectionRequest {
            name: name.to_owned(),
        };
        let req = collection_request_union::Request::DeleteCollection(req);
        self.inner.collection_union_call(req).await?;
        Ok(())
    }
}

struct DatabaseInner {
    name: String,
    client: Client,
}

impl DatabaseInner {
    fn new_txn(&self) -> DatabaseTxn {
        DatabaseTxn::new(self.name.clone(), self.client.clone())
    }

    fn new_collection<T: Object>(&self, name: String) -> Collection<T> {
        Collection::new(name, self.name.clone(), self.client.clone())
    }

    async fn database_union_call(
        &self,
        req: database_request_union::Request,
    ) -> Result<database_response_union::Response> {
        self.client.database_union(req).await
    }

    async fn collection_union_call(
        &self,
        req: collection_request_union::Request,
    ) -> Result<collection_response_union::Response> {
        self.client.collection_union(self.name.clone(), req).await
    }
}
