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
pub struct TxnClient {
    client: txn_client::TxnClient<Channel>,
}

impl TxnClient {
    pub fn new(chan: Channel) -> Self {
        let client = txn_client::TxnClient::new(chan);
        Self { client }
    }

    pub async fn call(&mut self, req: TxnRequest) -> Result<TxnResponse> {
        let res = self.client.call(req).await?;
        Ok(res.into_inner())
    }

    pub async fn database_call(&mut self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let req = TxnRequest {
            requests: vec![req],
        };
        let mut res = self.call(req).await?;
        res.responses.pop().ok_or(Error::InvalidResponse)
    }

    pub async fn collection_call(
        &mut self,
        database_id: u64,
        req: CollectionTxnRequest,
    ) -> Result<CollectionTxnResponse> {
        let req = DatabaseTxnRequest {
            database_id,
            collections: vec![req],
        };
        let mut res = self.database_call(req).await?;
        res.collections.pop().ok_or(Error::InvalidResponse)
    }

    pub async fn method_call(
        &mut self,
        database_id: u64,
        collection_id: u64,
        expr: MethodCallExpr,
    ) -> Result<Option<GenericValue>> {
        let req = CollectionTxnRequest {
            collection_id,
            exprs: vec![expr],
        };
        let mut res = self.collection_call(database_id, req).await?;
        Ok(res.values.pop())
    }
}
