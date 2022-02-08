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
use tonic::transport::Channel;

use crate::{Error, Result};

#[derive(Clone)]
pub struct UniverseClient {
    client: universe_client::UniverseClient<Channel>,
}

impl UniverseClient {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let client = universe_client::UniverseClient::connect(url.into())
            .await
            .map_err(Error::unknown)?;
        Ok(Self { client })
    }

    pub async fn databases(&mut self, req: DatabasesRequest) -> Result<DatabasesResponse> {
        let res = self.client.databases(req).await?;
        Ok(res.into_inner())
    }

    pub async fn databases_union(
        &mut self,
        req: databases_request_union::Request,
    ) -> Result<databases_response_union::Response> {
        let req = DatabasesRequest {
            request: Some(DatabasesRequestUnion { request: Some(req) }),
        };
        let res = self.databases(req).await?;
        res.response
            .and_then(|x| x.response)
            .ok_or(Error::InvalidResponse)
    }

    pub async fn collections(&mut self, req: CollectionsRequest) -> Result<CollectionsResponse> {
        let res = self.client.collections(req).await?;
        Ok(res.into_inner())
    }

    pub async fn collections_union(
        &mut self,
        database_id: u64,
        req: collections_request_union::Request,
    ) -> Result<collections_response_union::Response> {
        let req = CollectionsRequest {
            database_id,
            request: Some(CollectionsRequestUnion { request: Some(req) }),
        };
        let res = self.collections(req).await?;
        res.response
            .and_then(|x| x.response)
            .ok_or(Error::InvalidResponse)
    }
}
