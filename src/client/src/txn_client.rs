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

    pub async fn batch(&mut self, req: BatchTxnRequest) -> Result<BatchTxnResponse> {
        let res = self.client.batch(req).await?;
        Ok(res.into_inner())
    }

    pub async fn database(&mut self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let req = BatchTxnRequest {
            requests: vec![req],
        };
        let mut res = self.batch(req).await?;
        res.responses.pop().ok_or(Error::InvalidResponse)
    }

    pub async fn collection(
        &mut self,
        dbname: String,
        collection: CollectionTxnRequest,
    ) -> Result<CollectionTxnResponse> {
        let mut responses = self.collections(dbname, vec![collection]).await?;
        responses.pop().ok_or(Error::InvalidResponse)
    }

    pub async fn collections(
        &mut self,
        dbname: String,
        collections: Vec<CollectionTxnRequest>,
    ) -> Result<Vec<CollectionTxnResponse>> {
        let req = DatabaseTxnRequest {
            name: dbname,
            collections,
        };
        let res = self.database(req).await?;
        Ok(res.collections)
    }

    pub async fn collection_expr(
        &mut self,
        dbname: String,
        coname: String,
        expr: Expr,
    ) -> Result<ExprResult> {
        let mut results = self.collection_exprs(dbname, coname, vec![expr]).await?;
        results.pop().ok_or(Error::InvalidResponse)
    }

    pub async fn collection_exprs(
        &mut self,
        dbname: String,
        coname: String,
        exprs: Vec<Expr>,
    ) -> Result<Vec<ExprResult>> {
        let req = CollectionTxnRequest {
            name: coname,
            exprs,
        };
        let res = self.collection(dbname, req).await?;
        Ok(res.results)
    }
}
