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

use crate::{
    expr::{call_expr, subcall_expr},
    txn_client::TxnClient,
    Error, Result, Txn,
};

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
        self.call(call_expr::add_assign(value)).await?;
        Ok(())
    }

    pub async fn sub(self, value: impl Into<Value>) -> Result<()> {
        self.call(call_expr::sub_assign(value)).await?;
        Ok(())
    }

    pub async fn len(self) -> Result<i64> {
        let value = self.call(call_expr::len()).await?;
        if let Some(v) = value {
            v.as_i64().ok_or(Error::InvalidResponse)
        } else {
            Ok(0)
        }
    }

    pub async fn pop(self) -> Result<Option<Value>> {
        self.call(call_expr::pop()).await
    }

    pub async fn push(self, value: impl Into<Value>) -> Result<()> {
        self.call(call_expr::push(value)).await?;
        Ok(())
    }

    pub async fn append(self, value: impl Into<Value>) -> Result<()> {
        self.call(call_expr::append(value)).await?;
        Ok(())
    }

    pub async fn get(self, index: impl Into<Value>) -> Result<Option<Value>> {
        self.subcall(subcall_expr::get(index.into())).await
    }

    pub async fn set(self, index: impl Into<Value>, value: impl Into<Value>) -> Result<()> {
        self.subcall(subcall_expr::set(index.into(), value.into()))
            .await?;
        Ok(())
    }

    pub async fn remove(self, index: impl Into<Value>) -> Result<()> {
        self.subcall(subcall_expr::remove(index.into())).await?;
        Ok(())
    }

    async fn call(mut self, call: CallExpr) -> Result<Option<Value>> {
        let expr = engula_apis::Expr {
            id: self.id,
            call: Some(call),
            ..Default::default()
        };
        let result = self
            .client
            .collection_expr(self.dbname, self.coname, expr)
            .await?;
        Ok(result.value)
    }

    async fn subcall(mut self, call: CallExpr) -> Result<Option<Value>> {
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
