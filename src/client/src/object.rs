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

use crate::{expr::call, txn_client::TxnClient, Error, ObjectTxn, Result, Value};

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

    pub fn begin(self) -> ObjectTxn {
        ObjectTxn::new_owned(self.id, self.dbname, self.coname, self.client)
    }

    pub async fn get(mut self) -> Result<Value> {
        let method = MethodCallExpr {
            index: Some(method_call_expr::Index::BlobIdent(self.id)),
            call: Some(call::get()),
            ..Default::default()
        };
        let result = self.client.method(self.dbname, self.coname, method).await?;
        result.value.map(|x| x.into()).ok_or(Error::InvalidResponse)
    }

    pub async fn set(self, value: impl Into<Value>) -> Result<()> {
        self.begin().set(value).commit().await
    }

    pub async fn add(self, value: impl Into<Value>) -> Result<()> {
        self.begin().add(value).commit().await
    }

    pub async fn delete(self) -> Result<()> {
        self.begin().delete().commit().await
    }
}
