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

use engula_api::{server::v1::*, v1::*};
use futures::Stream;
use tonic::{Request, Response, Status};

use crate::{
    service::root::watch_response::{delete_event, update_event, DeleteEvent, UpdateEvent},
    Error, Result, Server,
};

type WatchStream = std::pin::Pin<
    Box<dyn Stream<Item = std::result::Result<WatchResponse, tonic::Status>> + Send + Sync>,
>;

#[tonic::async_trait]
impl root_server::Root for Server {
    type WatchStream = WatchStream;

    async fn admin(
        &self,
        req: Request<AdminRequest>,
    ) -> std::result::Result<Response<AdminResponse>, Status> {
        if !self.root.is_root() {
            todo!("forward")
        }
        let req = req.into_inner();
        let res = self.handle_admin(req).await?;
        Ok(Response::new(res))
    }

    async fn watch(
        &self,
        req: Request<WatchRequest>,
    ) -> std::result::Result<Response<Self::WatchStream>, Status> {
        if !self.root.is_root() {
            todo!("forward")
        }
        let req = req.into_inner();
        let cf = ChangeFetcher { seq: req.sequence };
        Ok(Response::new(cf.into_stream()))
    }

    async fn join(
        &self,
        request: Request<JoinNodeRequest>,
    ) -> std::result::Result<Response<JoinNodeResponse>, Status> {
        let request = request.into_inner();
        let schema = self.root.schema()?;
        let node = schema
            .add_node(NodeDesc {
                addr: request.addr,
                ..Default::default()
            })
            .await?;
        let cluster_id = schema.cluster_id().await?.unwrap();
        if let Some(prev_cluster_id) = request.cluster_id {
            if prev_cluster_id != cluster_id {
                return Err(Error::ClusterNotMatch.into());
            }
        }
        Ok(Response::new(JoinNodeResponse {
            cluster_id,
            node_id: node.id,
        }))
    }
}

impl Server {
    async fn handle_admin(&self, req: AdminRequest) -> Result<AdminResponse> {
        let mut res = AdminResponse::default();
        let req = req
            .request
            .ok_or_else(|| Error::InvalidArgument("AdminRequest".into()))?;
        res.response = Some(self.handle_admin_union(req).await?);
        Ok(res)
    }

    async fn handle_admin_union(&self, req: AdminRequestUnion) -> Result<AdminResponseUnion> {
        let req = req
            .request
            .ok_or_else(|| Error::InvalidArgument("AdminRequestUnion".into()))?;
        let res = match req {
            admin_request_union::Request::CreateDatabase(req) => {
                let res = self.handle_create_database(req).await?;
                admin_response_union::Response::CreateDatabase(res)
            }
            admin_request_union::Request::UpdateDatabase(_req) => {
                todo!()
            }
            admin_request_union::Request::DeleteDatabase(req) => {
                let res = self.handle_delete_database(req).await?;
                admin_response_union::Response::DeleteDatabase(res)
            }
            admin_request_union::Request::GetDatabase(req) => {
                let res = self.handle_get_database(req).await?;
                admin_response_union::Response::GetDatabase(res)
            }
            admin_request_union::Request::ListDatabases(_req) => {
                todo!()
            }
            admin_request_union::Request::CreateCollection(req) => {
                let res = self.handle_create_collection(req).await?;
                admin_response_union::Response::CreateCollection(res)
            }
            admin_request_union::Request::UpdateCollection(_req) => {
                todo!()
            }
            admin_request_union::Request::DeleteCollection(req) => {
                let res = self.handle_delete_collection(req).await?;
                admin_response_union::Response::DeleteCollection(res)
            }
            admin_request_union::Request::GetCollection(req) => {
                let res = self.handle_get_collection(req).await?;
                admin_response_union::Response::GetCollection(res)
            }
            admin_request_union::Request::ListCollections(_req) => {
                todo!()
            }
        };
        Ok(AdminResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse> {
        let desc = self
            .root
            .schema()?
            .create_database(DatabaseDesc {
                name: req.name,
                ..Default::default()
            })
            .await?;
        Ok(CreateDatabaseResponse {
            database: Some(desc),
        })
    }

    async fn handle_delete_database(
        &self,
        req: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse> {
        self.root.schema()?.delete_database(&req.name).await?;
        Ok(DeleteDatabaseResponse {})
    }

    async fn handle_get_database(&self, req: GetDatabaseRequest) -> Result<GetDatabaseResponse> {
        let resp = self.root.schema()?.get_database(&req.name).await?;
        Ok(GetDatabaseResponse { database: resp })
    }

    async fn handle_create_collection(
        &self,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        let schema = self.root.schema()?;
        let db = schema.get_database(&req.parent).await?;
        if db.is_none() {
            return Err(Error::DatabaseNotFound(req.parent));
        }
        let desc = schema
            .create_collection(CollectionDesc {
                name: req.name,
                parent_id: db.unwrap().id,
                ..Default::default()
            })
            .await?;
        Ok(CreateCollectionResponse {
            collection: Some(desc),
        })
    }

    async fn handle_delete_collection(
        &self,
        req: DeleteCollectionRequest,
    ) -> Result<DeleteCollectionResponse> {
        let schema = self.root.schema()?;
        let collection = schema.get_collection(&req.parent, &req.name).await?;
        if collection.is_none() {
            return Ok(DeleteCollectionResponse {});
        }
        schema.delete_collection(collection.unwrap().id).await?;
        Ok(DeleteCollectionResponse {})
    }

    async fn handle_get_collection(
        &self,
        req: GetCollectionRequest,
    ) -> Result<GetCollectionResponse> {
        let resp = self
            .root
            .schema()?
            .get_collection(&req.parent, &req.name)
            .await?;
        Ok(GetCollectionResponse { collection: resp })
    }
}

pub(crate) struct ChangeFetcher {
    seq: u64,
}

impl ChangeFetcher {
    pub(crate) fn into_stream(self) -> WatchStream {
        Box::pin(async_stream::stream! {
                yield Ok(WatchResponse {
                        sequence: self.seq,
                        updates: vec![UpdateEvent{event: Some(update_event::Event::Node(NodeDesc{id: 1, addr: "2".to_string()}))}],
                        deletes: vec![DeleteEvent{event: Some(delete_event::Event::Node(1))}],
                })
        })
    }
}
