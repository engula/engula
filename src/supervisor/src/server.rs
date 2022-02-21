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
use tonic::{Request, Response};

use crate::{apis::*, Database, Error, Result, Universe};

#[derive(Clone)]
pub struct Server {
    uv: Universe,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self {
            uv: Universe::new(),
        }
    }

    pub fn into_service(self) -> supervisor_server::SupervisorServer<Self> {
        supervisor_server::SupervisorServer::new(self)
    }
}

impl Server {
    async fn handle_database(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let mut res = DatabaseResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_database_union(req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_database_union(
        &self,
        req: DatabaseRequestUnion,
    ) -> Result<DatabaseResponseUnion> {
        let req = req
            .request
            .ok_or_else(|| Error::invalid_argument("missing database request"))?;
        let res = match req {
            database_request_union::Request::ListDatabases(_) => {
                todo!();
            }
            database_request_union::Request::CreateDatabase(req) => {
                let res = self.handle_create_database(req).await?;
                database_response_union::Response::CreateDatabase(res)
            }
            database_request_union::Request::UpdateDatabase(_) => {
                todo!();
            }
            database_request_union::Request::DeleteDatabase(_) => {
                todo!();
            }
            database_request_union::Request::DescribeDatabase(req) => {
                let res = self.handle_describe_database(req).await?;
                database_response_union::Response::DescribeDatabase(res)
            }
        };
        Ok(DatabaseResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse> {
        let desc = req
            .desc
            .ok_or_else(|| Error::invalid_argument("missing database description"))?;
        let desc = self.uv.create_database(desc).await?;
        Ok(CreateDatabaseResponse { desc: Some(desc) })
    }

    async fn handle_describe_database(
        &self,
        req: DescribeDatabaseRequest,
    ) -> Result<DescribeDatabaseResponse> {
        let db = self.uv.database(&req.name).await?;
        let desc = db.desc().await;
        Ok(DescribeDatabaseResponse { desc: Some(desc) })
    }

    async fn handle_collection(&self, req: CollectionRequest) -> Result<CollectionResponse> {
        let db = self.uv.database(&req.dbname).await?;
        let mut res = CollectionResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_collection_union(db.clone(), req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_collection_union(
        &self,
        db: Database,
        req: CollectionRequestUnion,
    ) -> Result<CollectionResponseUnion> {
        let req = req
            .request
            .ok_or_else(|| Error::invalid_argument("missing collection request"))?;
        let res = match req {
            collection_request_union::Request::ListCollections(_) => {
                todo!();
            }
            collection_request_union::Request::CreateCollection(req) => {
                let res = self.handle_create_collection(db, req).await?;
                collection_response_union::Response::CreateCollection(res)
            }
            collection_request_union::Request::UpdateCollection(_) => {
                todo!();
            }
            collection_request_union::Request::DeleteCollection(_) => {
                todo!();
            }
            collection_request_union::Request::DescribeCollection(req) => {
                let res = self.handle_describe_collection(db, req).await?;
                collection_response_union::Response::DescribeCollection(res)
            }
        };
        Ok(CollectionResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_collection(
        &self,
        db: Database,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        let desc = req
            .desc
            .ok_or_else(|| Error::invalid_argument("missing collection description"))?;
        let desc = db.create_collection(desc).await?;
        Ok(CreateCollectionResponse { desc: Some(desc) })
    }

    async fn handle_describe_collection(
        &self,
        db: Database,
        req: DescribeCollectionRequest,
    ) -> Result<DescribeCollectionResponse> {
        let co = db.collection(&req.name).await?;
        let desc = co.desc().await;
        Ok(DescribeCollectionResponse { desc: Some(desc) })
    }
}

#[tonic::async_trait]
impl supervisor_server::Supervisor for Server {
    async fn database(&self, req: Request<DatabaseRequest>) -> Result<Response<DatabaseResponse>> {
        let req = req.into_inner();
        let res = self.handle_database(req).await?;
        Ok(Response::new(res))
    }

    async fn collection(
        &self,
        req: Request<CollectionRequest>,
    ) -> Result<Response<CollectionResponse>> {
        let req = req.into_inner();
        let res = self.handle_collection(req).await?;
        Ok(Response::new(res))
    }
}
