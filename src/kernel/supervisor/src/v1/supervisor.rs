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

use super::{apis::v1::*, Error, Result, Universe};

#[derive(Clone)]
pub struct Supervisor {
    uv: Universe,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            uv: Universe::new(),
        }
    }

    pub async fn batch(&self, batch_req: BatchRequest) -> Result<BatchResponse> {
        let mut batch_res = BatchResponse::default();
        for req in batch_req.universes {
            let res = self.universe(req).await?;
            batch_res.universes.push(res);
        }
        Ok(batch_res)
    }

    async fn universe(&self, req: UniverseRequest) -> Result<UniverseResponse> {
        let req = req
            .request
            .ok_or_else(|| Error::invalid_argument("missing request"))?;
        let res = match req {
            universe_request::Request::ListDatabases(_) => {
                todo!();
            }
            universe_request::Request::CreateDatabase(req) => {
                let res = self.create_database(req).await?;
                universe_response::Response::CreateDatabase(res)
            }
            universe_request::Request::DescribeDatabase(req) => {
                let res = self.describe_database(req).await?;
                universe_response::Response::DescribeDatabase(res)
            }
            universe_request::Request::CreateCollection(req) => {
                let res = self.create_collection(req).await?;
                universe_response::Response::CreateCollection(res)
            }
            universe_request::Request::DescribeCollection(req) => {
                let res = self.describe_collection(req).await?;
                universe_response::Response::DescribeCollection(res)
            }
            _ => {
                todo!();
            }
        };
        Ok(UniverseResponse {
            response: Some(res),
        })
    }

    pub async fn create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse> {
        let db = self
            .uv
            .create_database(&req.name, req.options.unwrap_or_default())
            .await?;
        let desc = db.desc().await;
        Ok(CreateDatabaseResponse { desc: Some(desc) })
    }

    pub async fn describe_database(
        &self,
        req: DescribeDatabaseRequest,
    ) -> Result<DescribeDatabaseResponse> {
        let db = self.uv.database(&req.name).await?;
        let desc = db.desc().await;
        Ok(DescribeDatabaseResponse { desc: Some(desc) })
    }

    pub async fn create_collection(
        &self,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        let db = self.uv.database(&req.dbname).await?;
        let co = db
            .create_collection(&req.name, req.options.unwrap_or_default())
            .await?;
        let desc = co.desc().await;
        Ok(CreateCollectionResponse { desc: Some(desc) })
    }

    pub async fn describe_collection(
        &self,
        req: DescribeCollectionRequest,
    ) -> Result<DescribeCollectionResponse> {
        let db = self.uv.database(&req.dbname).await?;
        let co = db.collection(&req.name).await?;
        let desc = co.desc().await;
        Ok(DescribeCollectionResponse { desc: Some(desc) })
    }
}
