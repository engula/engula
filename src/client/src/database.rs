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
use tokio::sync::Mutex;

use crate::{txn_client::TxnClient, universe_client::UniverseClient, Collection, Error, Result};

pub struct Database {
    desc: DatabaseDesc,
    txn_client: TxnClient,
    universe_client: UniverseClient,
}

impl Database {
    pub fn new(desc: DatabaseDesc, txn_client: TxnClient, universe_client: UniverseClient) -> Self {
        Self {
            desc,
            txn_client,
            universe_client,
        }
    }

    pub async fn desc(&self) -> Result<DatabaseDesc> {
        let req = DescribeDatabaseRequest {
            id: self.desc.id,
            ..Default::default()
        };
        let req = databases_request_union::Request::DescribeDatabase(req);
        let res = self.universe_client.clone().databases_union(req).await?;
        if let databases_response_union::Response::DescribeDatabase(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub fn begin(&self) -> DatabaseTxn {
        DatabaseTxn::new(self.txn_client.clone(), self.desc.id)
    }

    pub async fn collection(&self, name: impl Into<String>) -> Result<Collection> {
        let desc = self.describe_collection(name).await?;
        Ok(Collection::new(
            desc,
            self.txn_client.clone(),
            self.universe_client.clone(),
        ))
    }

    pub async fn create_collection(&self, name: impl Into<String>) -> Result<Collection> {
        let spec = CollectionSpec {
            name: name.into(),
            ..Default::default()
        };
        let req = CreateCollectionRequest { spec: Some(spec) };
        let req = collections_request_union::Request::CreateCollection(req);
        let res = self
            .universe_client
            .clone()
            .collections_union(self.desc.id, req)
            .await?;
        if let collections_response_union::Response::CreateCollection(res) = res {
            let desc = res.desc.ok_or(Error::InvalidResponse)?;
            Ok(Collection::new(
                desc,
                self.txn_client.clone(),
                self.universe_client.clone(),
            ))
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub async fn delete_collection(&self, name: impl Into<String>) -> Result<()> {
        let req = DeleteCollectionRequest {
            name: name.into(),
            ..Default::default()
        };
        let req = collections_request_union::Request::DeleteCollection(req);
        self.universe_client
            .clone()
            .collections_union(self.desc.id, req)
            .await?;
        Ok(())
    }

    pub async fn describe_collection(&self, name: impl Into<String>) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest {
            name: name.into(),
            ..Default::default()
        };
        let req = collections_request_union::Request::DescribeCollection(req);
        let res = self
            .universe_client
            .clone()
            .collections_union(self.desc.id, req)
            .await?;
        if let collections_response_union::Response::DescribeCollection(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }
}

#[derive(Clone)]
pub struct DatabaseTxn {
    client: TxnClient,
    database_id: u64,
    collections: Arc<Mutex<Vec<CollectionTxnRequest>>>,
}

impl DatabaseTxn {
    fn new(client: TxnClient, database_id: u64) -> Self {
        Self {
            client,
            database_id,
            collections: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) async fn add(&self, co: CollectionTxnRequest) {
        self.collections.lock().await.push(co);
    }

    pub async fn commit(mut self) -> Result<()> {
        let collections = Arc::try_unwrap(self.collections)
            .map(|x| x.into_inner())
            .map_err(|_| Error::InvalidOperation("pending transactions".to_owned()))?;
        let req = DatabaseTxnRequest {
            database_id: self.database_id,
            collections,
        };
        self.client.database_call(req).await?;
        Ok(())
    }
}
