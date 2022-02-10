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

use engula_apis::{CallExpr, MethodCallExpr};

use crate::{expr::call_expr, txn_client::TxnClient, Result, Value};

#[allow(dead_code)]
pub struct Object {
    pub(crate) client: TxnClient,
    pub(crate) object_id: Vec<u8>,
    pub(crate) database_id: u64,
    pub(crate) collection_id: u64,
}

impl Object {
    async fn call(mut self, call_expr: CallExpr) -> Result<Value> {
        let method_expr = MethodCallExpr {
            id: self.object_id,
            call: Some(call_expr),
        };
        let value = self
            .client
            .method_call(self.database_id, self.collection_id, method_expr)
            .await?;
        Ok(value.into())
    }

    pub async fn get(self) -> Result<Value> {
        self.call(call_expr::get()).await
    }

    pub async fn set(self, value: impl Into<Value>) -> Result<()> {
        self.call(call_expr::set(value.into())).await?;
        Ok(())
    }

    pub async fn delete(self) -> Result<()> {
        self.call(call_expr::delete()).await?;
        Ok(())
    }

    pub async fn add(self, value: impl Into<Value>) -> Result<()> {
        self.call(call_expr::add(value.into())).await?;
        Ok(())
    }
}
