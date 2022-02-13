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
    pub fn new(chan: Channel) -> Self {
        let client = universe_client::UniverseClient::new(chan);
        Self { client }
    }

    pub async fn database(&mut self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let res = self.client.database(req).await?;
        Ok(res.into_inner())
    }

    pub async fn database_union(
        &mut self,
        req: database_request_union::Request,
    ) -> Result<database_response_union::Response> {
        let req = DatabaseRequest {
            requests: vec![DatabaseRequestUnion { request: Some(req) }],
        };
        let mut res = self.database(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or(Error::InvalidResponse)
    }

    pub async fn collection(&mut self, req: CollectionRequest) -> Result<CollectionResponse> {
        let res = self.client.collection(req).await?;
        Ok(res.into_inner())
    }

    pub async fn collection_union(
        &mut self,
        dbname: String,
        req: collection_request_union::Request,
    ) -> Result<collection_response_union::Response> {
        let req = CollectionRequest {
            dbname,
            requests: vec![CollectionRequestUnion { request: Some(req) }],
        };
        let mut res = self.collection(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or(Error::InvalidResponse)
    }
}
