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

use super::{Error, Result};

#[derive(Clone)]
pub struct Client {
    client: EngulaClient<Channel>,
}

impl Client {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let client = EngulaClient::connect(url.into()).await?;
        Ok(Self { client })
    }

    pub async fn batch_txn(&self, req: BatchTxnRequest) -> Result<BatchTxnResponse> {
        let res = self.client.clone().txn(req).await?;
        Ok(res.into_inner())
    }

    pub async fn select(&self, select: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let req = BatchTxnRequest {
            selects: vec![select],
            ..Default::default()
        };
        let mut res = self.batch_txn(req).await?;
        res.selects
            .pop()
            .ok_or_else(|| Error::internal("missing select response"))
    }

    pub async fn mutate(&self, mutate: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let req = BatchTxnRequest {
            mutates: vec![mutate],
            ..Default::default()
        };
        let mut res = self.batch_txn(req).await?;
        res.mutates
            .pop()
            .ok_or_else(|| Error::internal("missing mutate response"))
    }

    pub async fn batch_universe(&self, req: BatchUniverseRequest) -> Result<BatchUniverseResponse> {
        let res = self.client.clone().universe(req).await?;
        Ok(res.into_inner())
    }

    pub async fn universe(
        &self,
        req: universe_request::Request,
    ) -> Result<universe_response::Response> {
        let req = BatchUniverseRequest {
            requests: vec![UniverseRequest { request: Some(req) }],
        };
        let mut res = self.batch_universe(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing universe response"))
    }
}
