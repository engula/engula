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

use crate::{expr::call, txn_client::TxnClient, Error, Result, Txn};

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

    pub async fn add(self, value: impl Into<Value>) -> Result<()> {
        self.call(call().add(value)).await?;
        Ok(())
    }

    pub async fn sub(self, value: impl Into<Value>) -> Result<()> {
        self.call(call().sub(value)).await?;
        Ok(())
    }

    pub async fn len(self) -> Result<i64> {
        let value = self.call(call().len()).await?;
        if let Some(v) = value {
            v.as_i64().ok_or(Error::InvalidResponse)
        } else {
            Ok(0)
        }
    }

    pub async fn append(self, value: impl Into<Value>) -> Result<()> {
        self.call(call().append(value)).await?;
        Ok(())
    }

    pub async fn pop_back(self) -> Result<Option<Value>> {
        self.call(call().pop_back()).await
    }

    pub async fn pop_front(self) -> Result<Option<Value>> {
        self.call(call().pop_front()).await
    }

    pub async fn push_back(self, value: impl Into<Value>) -> Result<()> {
        self.call(call().push_back(value)).await?;
        Ok(())
    }

    pub async fn push_front(self, value: impl Into<Value>) -> Result<()> {
        self.call(call().push_front(value)).await?;
        Ok(())
    }

    pub async fn get(self, index: impl Into<Value>) -> Result<Option<Value>> {
        self.call(call().get(index)).await
    }

    pub async fn set(self, index: impl Into<Value>, value: impl Into<Value>) -> Result<()> {
        self.call(call().set(index, value)).await?;
        Ok(())
    }

    pub async fn delete(self, index: impl Into<Value>) -> Result<()> {
        self.call(call().delete(index)).await?;
        Ok(())
    }

    async fn call(mut self, call: CallExpr) -> Result<Option<Value>> {
        let expr = engula_apis::Expr {
            id: self.id,
            subcalls: vec![call],
            ..Default::default()
        };
        let result = self
            .client
            .collection_expr(self.dbname, self.coname, expr)
            .await?;
        Ok(result.value)
    }
}
