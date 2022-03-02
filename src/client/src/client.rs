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
pub struct Client {
    client: engula_client::EngulaClient<Channel>,
}

impl Client {
    pub async fn connect(url: String) -> Result<Self> {
        let client = engula_client::EngulaClient::connect(url)
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        Ok(Self { client })
    }

    pub async fn txn(&self, req: TxnRequest) -> Result<TxnResponse> {
        let res = self.client.clone().txn(req).await?;
        Ok(res.into_inner())
    }

    pub async fn database_txn(&self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let req = TxnRequest {
            requests: vec![req],
            ..Default::default()
        };
        let mut res = self.txn(req).await?;
        res.responses
            .pop()
            .ok_or_else(|| Error::internal("missing database response"))
    }

    pub async fn collection_txn(
        &self,
        dbname: String,
        req: CollectionTxnRequest,
    ) -> Result<CollectionTxnResponse> {
        let req = DatabaseTxnRequest {
            name: dbname,
            requests: vec![req],
        };
        let mut res = self.database_txn(req).await?;
        res.responses
            .pop()
            .ok_or_else(|| Error::internal("missing collection response"))
    }

    pub async fn collection_expr(
        &self,
        dbname: String,
        coname: String,
        expr: Expr,
    ) -> Result<ExprResult> {
        let req = CollectionTxnRequest {
            name: coname,
            exprs: vec![expr],
        };
        let mut res = self.collection_txn(dbname, req).await?;
        res.results
            .pop()
            .ok_or_else(|| Error::internal("missing expression result"))
    }

    pub async fn database(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let res = self.client.clone().database(req).await?;
        Ok(res.into_inner())
    }

    pub async fn database_union(
        &self,
        req: database_request_union::Request,
    ) -> Result<database_response_union::Response> {
        let req = DatabaseRequest {
            requests: vec![DatabaseRequestUnion { request: Some(req) }],
        };
        let mut res = self.database(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing database response"))
    }

    pub async fn collection(&self, req: CollectionRequest) -> Result<CollectionResponse> {
        let res = self.client.clone().collection(req).await?;
        Ok(res.into_inner())
    }

    pub async fn collection_union(
        &self,
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
            .ok_or_else(|| Error::internal("missing collection response"))
    }
}
