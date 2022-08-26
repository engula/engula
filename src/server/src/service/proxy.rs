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

use ::engula_client::{Collection, Database};
use engula_api::v1::*;
use tonic::{Request, Response, Status};

use super::ProxyServer;
use crate::{record_latency, service::metrics::take_database_request_metrics, Error};

#[tonic::async_trait]
impl engula_server::Engula for ProxyServer {
    async fn admin(
        &self,
        request: Request<AdminRequest>,
    ) -> Result<Response<AdminResponse>, Status> {
        use engula_api::v1::{admin_request_union::Request, admin_response_union::Response};
        let req = request
            .into_inner()
            .request
            .and_then(|r| r.request)
            .ok_or_else(|| {
                Error::InvalidArgument(
                    "AdminRequest::request or AdminRequestUnion::request is required".to_owned(),
                )
            })?;
        let resp = match req {
            Request::GetDatabase(req) => Response::GetDatabase(self.get_database(req).await?),
            Request::ListDatabases(req) => Response::ListDatabases(self.list_database(req).await?),
            Request::CreateDatabase(req) => {
                Response::CreateDatabase(self.create_database(req).await?)
            }
            Request::UpdateDatabase(req) => {
                Response::UpdateDatabase(self.update_database(req).await?)
            }
            Request::DeleteDatabase(req) => {
                Response::DeleteDatabase(self.delete_database(req).await?)
            }
            Request::GetCollection(req) => Response::GetCollection(self.get_collection(req).await?),
            Request::ListCollections(req) => {
                Response::ListCollections(self.list_collections(req).await?)
            }
            Request::CreateCollection(req) => {
                Response::CreateCollection(self.create_collection(req).await?)
            }
            Request::UpdateCollection(req) => {
                Response::UpdateCollection(self.update_collection(req).await?)
            }
            Request::DeleteCollection(req) => {
                Response::DeleteCollection(self.delete_collection(req).await?)
            }
        };

        Ok(tonic::Response::new(AdminResponse {
            response: Some(AdminResponseUnion {
                response: Some(resp),
            }),
        }))
    }

    async fn database(
        &self,
        request: Request<DatabaseRequest>,
    ) -> Result<Response<DatabaseResponse>, Status> {
        use engula_api::v1::{
            collection_request_union::Request, collection_response_union::Response,
        };

        let request = request.into_inner();
        let request = request.request.ok_or_else(|| {
            Error::InvalidArgument("DatabaseRequest::request is required".to_owned())
        })?;
        let collection = request.collection.ok_or_else(|| {
            Error::InvalidArgument("CollectionRequest::collection is required".to_owned())
        })?;
        let request = request.request.and_then(|r| r.request).ok_or_else(|| {
            Error::InvalidArgument(
                "CollectionRequest::request or CollectionRequestUnion is required".to_owned(),
            )
        })?;
        record_latency!(take_database_request_metrics(&request));
        let resp = match request {
            Request::Get(req) => Response::Get(self.handle_get(collection, req).await?),
            Request::Put(req) => Response::Put(self.handle_put(collection, req).await?),
            Request::Delete(req) => Response::Delete(self.handle_delete(collection, req).await?),
        };
        Ok(tonic::Response::new(DatabaseResponse {
            response: Some(CollectionResponse {
                response: Some(CollectionResponseUnion {
                    response: Some(resp),
                }),
            }),
        }))
    }
}

impl ProxyServer {
    async fn get_database(&self, req: GetDatabaseRequest) -> Result<GetDatabaseResponse, Status> {
        let database = self.client.open_database(req.name).await?;
        Ok(GetDatabaseResponse {
            database: Some(database.desc()),
        })
    }

    async fn list_database(
        &self,
        _req: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, Status> {
        let databases = self
            .client
            .list_database()
            .await?
            .into_iter()
            .map(|d| d.desc())
            .collect();
        Ok(ListDatabasesResponse { databases })
    }

    async fn create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, Status> {
        let database = self.client.create_database(req.name).await?;
        Ok(CreateDatabaseResponse {
            database: Some(database.desc()),
        })
    }

    async fn update_database(
        &self,
        _req: UpdateDatabaseRequest,
    ) -> Result<UpdateDatabaseResponse, Status> {
        Err(Status::unimplemented("ProxyServer::update_database"))
    }

    async fn delete_database(
        &self,
        req: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse, Status> {
        self.client.delete_database(req.name).await?;
        Ok(DeleteDatabaseResponse {})
    }

    async fn get_collection(
        &self,
        req: GetCollectionRequest,
    ) -> Result<GetCollectionResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("GetCollectionRequest::database is required".to_owned())
        })?;
        let name = req.name;
        let database = Database::new(self.client.clone(), desc, None);
        let collection = database.open_collection(name).await?;
        Ok(GetCollectionResponse {
            collection: Some(collection.desc()),
        })
    }

    async fn list_collections(
        &self,
        req: ListCollectionsRequest,
    ) -> Result<ListCollectionsResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("ListCollectionRequest::database is required".to_owned())
        })?;
        let database = Database::new(self.client.clone(), desc, None);
        let collections = database
            .list_collection()
            .await?
            .into_iter()
            .map(|c| c.desc())
            .collect();
        Ok(ListCollectionsResponse { collections })
    }

    async fn create_collection(
        &self,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("CreateCollectionRequest::database is required".to_owned())
        })?;
        let partition = req.partition.ok_or_else(|| {
            Error::InvalidArgument("CreateCollectionRequest::partition is required".to_owned())
        })?;
        let name = req.name;
        let database = Database::new(self.client.clone(), desc, None);
        let collection = database
            .create_collection(name, Some(partition.into()))
            .await?;
        Ok(CreateCollectionResponse {
            collection: Some(collection.desc()),
        })
    }

    async fn update_collection(
        &self,
        _req: UpdateCollectionRequest,
    ) -> Result<UpdateCollectionResponse, Status> {
        Err(Status::unimplemented("ProxyServer::update_collection"))
    }

    async fn delete_collection(
        &self,
        req: DeleteCollectionRequest,
    ) -> Result<DeleteCollectionResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("DeleteCollectionRequest::database is required".to_owned())
        })?;
        let name = req.name;
        let database = Database::new(self.client.clone(), desc, None);
        database.delete_collection(name).await?;
        Ok(DeleteCollectionResponse {})
    }
}

impl ProxyServer {
    async fn handle_get(
        &self,
        desc: CollectionDesc,
        req: GetRequest,
    ) -> Result<GetResponse, Status> {
        let collection = Collection::new(self.client.clone(), desc, None);
        let resp = collection.get(req.key).await?;
        Ok(GetResponse { value: resp })
    }

    async fn handle_put(
        &self,
        desc: CollectionDesc,
        req: PutRequest,
    ) -> Result<PutResponse, Status> {
        let collection = Collection::new(self.client.clone(), desc, None);
        collection.put(req.key, req.value).await?;
        Ok(PutResponse {})
    }

    async fn handle_delete(
        &self,
        desc: CollectionDesc,
        req: DeleteRequest,
    ) -> Result<DeleteResponse, Status> {
        let collection = Collection::new(self.client.clone(), desc, None);
        collection.delete(req.key).await?;
        Ok(DeleteResponse {})
    }
}
