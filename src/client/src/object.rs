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

use crate::{txn_client::TxnClient, Result};

#[allow(dead_code)]
pub struct Object {
    id: Vec<u8>,
    dbname: String,
    coname: String,
    client: TxnClient,
}

impl Object {
    pub(crate) fn new(id: Vec<u8>, dbname: String, coname: String, client: TxnClient) -> Self {
        Self {
            id,
            dbname,
            coname,
            client,
        }
    }

    pub(crate) async fn call(mut self, call: CallExpr) -> Result<Option<Value>> {
        let expr = engula_apis::Expr {
            id: self.id,
            call: Some(call),
        };
        let result = self
            .client
            .collection_expr(self.dbname, self.coname, expr)
            .await?;
        Ok(result.value)
    }
}
