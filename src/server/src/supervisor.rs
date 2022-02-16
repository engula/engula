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

use std::{collections::HashMap, sync::Arc};

use engula_apis::*;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{Error, Result};

type TonicResult<T> = std::result::Result<T, Status>;

pub struct Supervisor {
    uv: Universe,
}

impl Supervisor {
    fn new() -> Self {
        Self {
            uv: Universe::new(),
        }
    }

    pub fn new_service() -> universe_server::UniverseServer<Self> {
        universe_server::UniverseServer::new(Self::new())
    }
}

#[tonic::async_trait]
impl universe_server::Universe for Supervisor {
    async fn database(
        &self,
        req: Request<DatabaseRequest>,
    ) -> TonicResult<Response<DatabaseResponse>> {
        let req = req.into_inner();
        let res = self.handle_database(req).await?;
        Ok(Response::new(res))
    }

    async fn collection(
        &self,
        req: Request<CollectionRequest>,
    ) -> TonicResult<Response<CollectionResponse>> {
        let req = req.into_inner();
        let res = self.handle_collection(req).await?;
        Ok(Response::new(res))
    }
}

impl Supervisor {
    pub(crate) async fn handle_database(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let mut res = DatabaseResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_database_union(req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    pub(crate) async fn handle_database_union(
        &self,
        req: DatabaseRequestUnion,
    ) -> Result<DatabaseResponseUnion> {
        let req = req.request.ok_or(Error::InvalidRequest)?;
        let res = match req {
            database_request_union::Request::ListDatabases(_req) => {
                todo!();
            }
            database_request_union::Request::CreateDatabase(req) => {
                let res = self.handle_create_database(req).await?;
                database_response_union::Response::CreateDatabase(res)
            }
            database_request_union::Request::UpdateDatabase(_req) => {
                todo!();
            }
            database_request_union::Request::DeleteDatabase(_req) => {
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
        let desc = req.desc.ok_or(Error::InvalidRequest)?;
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
}

impl Supervisor {
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
        let req = req.request.ok_or(Error::InvalidRequest)?;
        let res = match req {
            collection_request_union::Request::ListCollections(_req) => {
                todo!();
            }
            collection_request_union::Request::CreateCollection(req) => {
                let res = self.handle_create_collection(db, req).await?;
                collection_response_union::Response::CreateCollection(res)
            }
            collection_request_union::Request::UpdateCollection(_req) => {
                todo!();
            }
            collection_request_union::Request::DeleteCollection(_req) => {
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
        let desc = req.desc.ok_or(Error::InvalidRequest)?;
        let desc = db.create_collection(desc).await?;
        Ok(CreateCollectionResponse { desc: Some(desc) })
    }

    async fn handle_describe_collection(
        &self,
        db: Database,
        req: DescribeCollectionRequest,
    ) -> Result<DescribeCollectionResponse> {
        let desc = db.collection(&req.name).await?;
        Ok(DescribeCollectionResponse { desc: Some(desc) })
    }
}

#[derive(Clone)]
pub struct Universe {
    inner: Arc<Mutex<UniverseInner>>,
}

struct UniverseInner {
    next_id: u64,
    databases: HashMap<String, Database>,
}

impl Universe {
    fn new() -> Self {
        let inner = UniverseInner {
            next_id: 1,
            databases: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    async fn database(&self, name: &str) -> Result<Database> {
        let inner = self.inner.lock().await;
        inner
            .databases
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("database {}", name)))
    }

    async fn create_database(&self, mut desc: DatabaseDesc) -> Result<DatabaseDesc> {
        let mut inner = self.inner.lock().await;
        if inner.databases.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("database {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        let db = Database::new(desc.clone());
        inner.databases.insert(desc.name.clone(), db);
        Ok(desc)
    }
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

struct DatabaseInner {
    desc: DatabaseDesc,
    next_id: u64,
    collections: HashMap<String, CollectionDesc>,
}

impl Database {
    fn new(desc: DatabaseDesc) -> Self {
        let inner = DatabaseInner {
            desc,
            next_id: 1,
            collections: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    async fn desc(&self) -> DatabaseDesc {
        self.inner.lock().await.desc.clone()
    }

    async fn collection(&self, name: &str) -> Result<CollectionDesc> {
        let inner = self.inner.lock().await;
        inner
            .collections
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("collection {}", name)))
    }

    async fn create_collection(&self, mut desc: CollectionDesc) -> Result<CollectionDesc> {
        let mut inner = self.inner.lock().await;
        if inner.collections.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("collection {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        desc.parent_id = inner.desc.id;
        inner.collections.insert(desc.name.clone(), desc.clone());
        Ok(desc)
    }
}
