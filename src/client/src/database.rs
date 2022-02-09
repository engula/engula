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

use crate::{universe_client::UniverseClient, Collection, Error, Result};

pub struct Database {
    client: UniverseClient,
    desc: DatabaseDesc,
}

impl Database {
    pub fn new(client: UniverseClient, desc: DatabaseDesc) -> Self {
        Self { client, desc }
    }

    pub async fn desc(&self) -> Result<DatabaseDesc> {
        let req = DescribeDatabaseRequest {
            id: self.desc.id,
            ..Default::default()
        };
        let req = databases_request_union::Request::DescribeDatabase(req);
        let res = self.client.clone().databases_union(req).await?;
        if let databases_response_union::Response::DescribeDatabase(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub fn begin(&self) -> DatabaseTxn {
        todo!();
    }

    pub async fn collection(&self, name: impl Into<String>) -> Result<Collection> {
        let desc = self.describe_collection(name).await?;
        Ok(Collection::new(self.client.clone(), desc))
    }

    pub async fn create_collection(&self, name: impl Into<String>) -> Result<Collection> {
        let spec = CollectionSpec {
            name: name.into(),
            ..Default::default()
        };
        let req = CreateCollectionRequest { spec: Some(spec) };
        let req = collections_request_union::Request::CreateCollection(req);
        let res = self
            .client
            .clone()
            .collections_union(self.desc.id, req)
            .await?;
        if let collections_response_union::Response::CreateCollection(res) = res {
            let desc = res.desc.ok_or(Error::InvalidResponse)?;
            Ok(Collection::new(self.client.clone(), desc))
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
        self.client
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
            .client
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

pub struct DatabaseTxn {}
