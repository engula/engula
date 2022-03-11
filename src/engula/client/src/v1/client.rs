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

use engula_apis::v1::{txn_client::TxnClient, universe_client::UniverseClient, *};
use tonic::transport::Channel;

use crate::{Error, Result};

#[derive(Clone)]
pub struct Client {
    txn: TxnClient<Channel>,
    universe: UniverseClient<Channel>,
}

impl Client {
    pub fn new(chan: Channel) -> Self {
        Self {
            txn: TxnClient::new(chan.clone()),
            universe: UniverseClient::new(chan),
        }
    }

    pub async fn batch_txn(&self, req: BatchTxnRequest) -> Result<BatchTxnResponse> {
        let res = self.txn.clone().batch(req).await?;
        Ok(res.into_inner())
    }

    pub async fn txn(&self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let req = BatchTxnRequest {
            requests: vec![req],
        };
        let mut res = self.batch_txn(req).await?;
        res.responses
            .pop()
            .ok_or_else(|| Error::internal("missing database response"))
    }

    pub async fn batch_universe(&self, req: BatchUniverseRequest) -> Result<BatchUniverseResponse> {
        let res = self.universe.clone().batch(req).await?;
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
