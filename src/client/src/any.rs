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

use crate::{expr::call, txn_client::TxnClient, ObjectValue, Result, Txn};

pub struct Any {
    id: Vec<u8>,
    dbname: String,
    coname: String,
    client: TxnClient,
}

impl Any {
    pub(crate) fn new(id: Vec<u8>, dbname: String, coname: String, client: TxnClient) -> Self {
        Self {
            id,
            dbname,
            coname,
            client,
        }
    }

    pub fn begin(self) -> Txn {
        Txn::new(self.id, self.dbname, self.coname, self.client)
    }

    pub async fn load(self) -> Result<Option<Value>> {
        self.call(call::load()).await
    }

    pub async fn store(self, value: impl Into<Value>) -> Result<()> {
        self.call(call::store(value)).await?;
        Ok(())
    }

    pub async fn reset(self) -> Result<()> {
        self.call(call::reset()).await?;
        Ok(())
    }

    pub async fn add(self, value: impl Into<Value>) -> Result<()> {
        self.call(call::add(value)).await?;
        Ok(())
    }

    pub async fn sub(self, value: impl Into<Value>) -> Result<()> {
        self.call(call::sub(value)).await?;
        Ok(())
    }

    pub async fn len(self) -> Result<Option<i64>> {
        let value = self.call(call::len()).await?;
        i64::cast_from_option(value)
    }

    pub async fn append(self, value: impl Into<Value>) -> Result<()> {
        self.call(call::append(value)).await?;
        Ok(())
    }

    pub async fn push_back(self, value: impl Into<Value>) -> Result<()> {
        self.call(call::push_back(value)).await?;
        Ok(())
    }

    pub async fn push_front(self, value: impl Into<Value>) -> Result<()> {
        self.call(call::push_front(value)).await?;
        Ok(())
    }

    async fn call(mut self, call: CallExpr) -> Result<Option<Value>> {
        let expr = Expr {
            from: Some(expr::From::Id(self.id)),
            call: Some(call),
            ..Default::default()
        };
        let mut result = self
            .client
            .collection_expr(self.dbname, self.coname, expr)
            .await?;
        Ok(result.values.pop().and_then(|v| v.into()))
    }
}
