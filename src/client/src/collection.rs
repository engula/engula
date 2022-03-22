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

use crate::{Any, Client, CollectionTxn, Error, Result};

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

    pub async fn get<T: TryFrom<Value>>(&self, id: impl Into<Vec<u8>>) -> Result<T> {
        self.select(id, Any::get()).await
    }

    pub async fn set(&self, id: impl Into<Vec<u8>>, value: impl Into<Value>) -> Result<()> {
        self.mutate(id, Any::set(value)).await
    }

    pub async fn delete(&self, id: impl Into<Vec<u8>>) -> Result<()> {
        self.mutate(id, Any::delete()).await?;
        Ok(())
    }

    pub async fn select<T: TryFrom<Value>>(
        &self,
        id: impl Into<Vec<u8>>,
        select: impl Into<SelectExpr>,
    ) -> Result<T> {
        let expr = ObjectExpr {
            batch: vec![id.into()],
            select: Some(select.into()),
            ..Default::default()
        };
        self.object(expr).await
    }

    pub async fn mutate<T: TryFrom<Value>>(
        &self,
        id: impl Into<Vec<u8>>,
        mutate: impl Into<MutateExpr>,
    ) -> Result<T> {
        let expr = ObjectExpr {
            batch: vec![id.into()],
            mutate: Some(mutate.into()),
            ..Default::default()
        };
        self.object(expr).await
    }

    async fn object<T: TryFrom<Value>>(&self, expr: ObjectExpr) -> Result<T> {
        let req = CollectionRequest {
            name: self.name.clone(),
            exprs: vec![expr],
        };
        let req = DatabaseRequest {
            name: self.dbname.clone(),
            requests: vec![req],
        };
        let mut res = self.client.database(req).await?;
        let mut res = res
            .responses
            .pop()
            .ok_or_else(|| Error::internal("missing collection response"))?;
        let value = res
            .results
            .pop()
            .and_then(|mut r| r.values.pop())
            .ok_or_else(|| Error::internal("missing expression result"))?;
        value.try_into().map_err(|_| Error::invalid_conversion())
    }
}
