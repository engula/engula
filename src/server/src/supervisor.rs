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
use tonic::{Request, Response, Status};

use crate::{database::Database, universe::Universe, Error, Result};

pub struct Supervisor {
    uv: Universe,
}

type TonicResult<T> = std::result::Result<T, Status>;

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

    pub fn into_service(self) -> universe_server::UniverseServer<Self> {
        universe_server::UniverseServer::new(self)
    }
}

#[tonic::async_trait]
impl universe_server::Universe for Supervisor {
    async fn databases(
        &self,
        req: Request<DatabasesRequest>,
    ) -> TonicResult<Response<DatabasesResponse>> {
        let req = req.into_inner();
        let res = self.handle_databases(req).await?;
        Ok(Response::new(res))
    }

    async fn collections(
        &self,
        req: Request<CollectionsRequest>,
    ) -> TonicResult<Response<CollectionsResponse>> {
        let req = req.into_inner();
        let res = self.handle_collections(req).await?;
        Ok(Response::new(res))
    }
}

impl Supervisor {
    async fn handle_databases(&self, req: DatabasesRequest) -> Result<DatabasesResponse> {
        let req = req
            .request
            .and_then(|x| x.request)
            .ok_or(Error::InvalidRequest)?;
        let res = match req {
            databases_request_union::Request::ListDatabases(_req) => {
                todo!();
            }
            databases_request_union::Request::CreateDatabase(req) => {
                let res = self.handle_create_database(req).await?;
                databases_response_union::Response::CreateDatabase(res)
            }
            databases_request_union::Request::UpdateDatabase(_req) => {
                todo!();
            }
            databases_request_union::Request::DeleteDatabase(_req) => {
                todo!();
            }
            databases_request_union::Request::DescribeDatabase(req) => {
                let res = self.handle_describe_database(req).await?;
                databases_response_union::Response::DescribeDatabase(res)
            }
        };
        let res = DatabasesResponseUnion {
            response: Some(res),
        };
        Ok(DatabasesResponse {
            response: Some(res),
        })
    }

    async fn handle_create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse> {
        let spec = req.spec.ok_or(Error::InvalidRequest)?;
        let desc = self.uv.create_database(spec).await?;
        Ok(CreateDatabaseResponse { desc: Some(desc) })
    }

    async fn handle_describe_database(
        &self,
        req: DescribeDatabaseRequest,
    ) -> Result<DescribeDatabaseResponse> {
        let db = if req.id > 0 {
            self.uv.database(req.id).await?
        } else {
            self.uv.lookup_database(&req.name).await?
        };
        let desc = db.desc().await;
        Ok(DescribeDatabaseResponse { desc: Some(desc) })
    }
}

impl Supervisor {
    async fn handle_collections(&self, req: CollectionsRequest) -> Result<CollectionsResponse> {
        let db = self.uv.database(req.database_id).await?;
        let req = req
            .request
            .and_then(|x| x.request)
            .ok_or(Error::InvalidRequest)?;
        let res = match req {
            collections_request_union::Request::ListCollections(_req) => {
                todo!();
            }
            collections_request_union::Request::CreateCollection(req) => {
                let res = self.handle_create_collection(db, req).await?;
                collections_response_union::Response::CreateCollection(res)
            }
            collections_request_union::Request::UpdateCollection(_req) => {
                todo!();
            }
            collections_request_union::Request::DeleteCollection(_req) => {
                todo!();
            }
            collections_request_union::Request::DescribeCollection(req) => {
                let res = self.handle_describe_collection(db, req).await?;
                collections_response_union::Response::DescribeCollection(res)
            }
        };
        let res = CollectionsResponseUnion {
            response: Some(res),
        };
        Ok(CollectionsResponse {
            response: Some(res),
        })
    }

    async fn handle_create_collection(
        &self,
        db: Database,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        let spec = req.spec.ok_or(Error::InvalidRequest)?;
        let desc = db.create_collection(spec).await?;
        Ok(CreateCollectionResponse { desc: Some(desc) })
    }

    async fn handle_describe_collection(
        &self,
        db: Database,
        req: DescribeCollectionRequest,
    ) -> Result<DescribeCollectionResponse> {
        let desc = if req.id > 0 {
            db.collection(req.id).await?
        } else {
            db.lookup_collection(&req.name).await?
        };
        Ok(DescribeCollectionResponse { desc: Some(desc) })
    }
}
