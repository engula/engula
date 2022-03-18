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

use engula_apis::v1::{engula_client::EngulaClient, *};
use tonic::transport::Channel;

use crate::{Error, Result};

#[derive(Clone)]
pub struct Client {
    client: EngulaClient<Channel>,
}

impl Client {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let client = EngulaClient::connect(url.into()).await?;
        Ok(Self { client })
    }

    pub async fn batch(&self, req: BatchRequest) -> Result<BatchResponse> {
        let res = self.client.clone().batch(req).await?;
        Ok(res.into_inner())
    }

    pub async fn database(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let req = BatchRequest {
            databases: vec![req],
            ..Default::default()
        };
        let mut res = self.batch(req).await?;
        res.databases
            .pop()
            .ok_or_else(|| Error::internal("missing database response"))
    }

    pub async fn universe(
        &self,
        req: universe_request::Request,
    ) -> Result<universe_response::Response> {
        let req = BatchRequest {
            universes: vec![UniverseRequest { request: Some(req) }],
            ..Default::default()
        };
        let mut res = self.batch(req).await?;
        res.universes
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing universe response"))
    }
}
