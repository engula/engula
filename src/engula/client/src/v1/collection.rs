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

use engula_apis::v1::*;

use super::{Any, Client, CollectionTxn, Error, MutateExpr, Result, SelectExpr};

#[derive(Clone)]
pub struct Collection {
    name: String,
    dbname: String,
    client: Client,
}

impl Collection {
    pub(crate) fn new(name: String, dbname: String, client: Client) -> Self {
        Self {
            name,
            dbname,
            client,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dbname(&self) -> &str {
        &self.dbname
    }

    pub async fn desc(&self) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest {
            name: self.name.clone(),
            dbname: self.dbname.clone(),
        };
        let req = universe_request::Request::DescribeCollection(req);
        let res = self.client.universe(req).await?;
        let desc = if let universe_response::Response::DescribeCollection(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing collection descriptor"))
    }

    pub fn begin(&self) -> CollectionTxn {
        CollectionTxn::new(self.name.clone(), self.dbname.clone(), self.client.clone())
    }

    pub async fn object<T: TryFrom<TypedValue>>(&self, id: impl Into<Vec<u8>>) -> Result<T> {
        self.select(id, Any::get()).await
    }

    pub async fn select<T: TryFrom<TypedValue>>(
        &self,
        id: impl Into<Vec<u8>>,
        expr: impl Into<SelectExpr>,
    ) -> Result<T> {
        let req = CollectionTxnRequest {
            name: self.name.clone(),
            ids: vec![id.into()],
            exprs: vec![expr.into().into()],
        };
        let req = DatabaseTxnRequest {
            name: self.dbname.clone(),
            requests: vec![req],
        };
        let mut res = self.client.select(req).await?;
        let value = res
            .responses
            .pop()
            .and_then(|mut x| x.values.pop())
            .ok_or_else(|| Error::internal("missing select result"))?;
        value.try_into().map_err(|_| Error::invalid_conversion())
    }

    pub async fn mutate<T: TryFrom<TypedValue>>(
        &self,
        id: impl Into<Vec<u8>>,
        expr: impl Into<MutateExpr>,
    ) -> Result<T> {
        let req = CollectionTxnRequest {
            name: self.name.clone(),
            ids: vec![id.into()],
            exprs: vec![expr.into().into()],
        };
        let req = DatabaseTxnRequest {
            name: self.dbname.clone(),
            requests: vec![req],
        };
        let mut res = self.client.mutate(req).await?;
        let value = res
            .responses
            .pop()
            .and_then(|mut x| x.values.pop())
            .ok_or_else(|| Error::internal("missing mutate result"))?;
        value.try_into().map_err(|_| Error::invalid_conversion())
    }

    pub async fn delete(&self, id: impl Into<Vec<u8>>) -> Result<()> {
        self.mutate(id, Any::delete()).await?;
        Ok(())
    }
}
