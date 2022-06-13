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

use std::sync::Arc;

use engula_api::{server::v1::*, v1::*};
use futures::Stream;
use tonic::{Request, Response, Status};

use crate::{root::Schema, service::root::watch_response::UpdateEvent, Error, Result, Server};

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
        let req = req.into_inner();
        let res = self.handle_admin(req).await?;
        Ok(Response::new(res))
    }

    async fn watch(
        &self,
        req: Request<WatchRequest>,
    ) -> std::result::Result<Response<Self::WatchStream>, Status> {
        let req = req.into_inner();
        let seq = req.sequence;
        let schema = self.schema().await?;
        let updates = schema.list_all_events(seq).await?;
        Ok(Response::new(ListStream { seq, updates }.into_stream()))
    }

    async fn join(
        &self,
        request: Request<JoinNodeRequest>,
    ) -> std::result::Result<Response<JoinNodeResponse>, Status> {
        let request = request.into_inner();
        let schema = self.schema().await?;
        let node = schema
            .add_node(NodeDesc {
                addr: request.addr,
                ..Default::default()
            })
            .await?;
        let cluster_id = schema.cluster_id().await?.unwrap();
        self.address_resolver.insert(&node);

        let mut roots = schema.get_root_replicas().await?;
        roots.move_first(node.id);

        Ok::<Response<JoinNodeResponse>, Status>(Response::new(JoinNodeResponse {
            cluster_id,
            node_id: node.id,
            roots: roots.into(),
        }))
    }

    async fn resolve(
        &self,
        request: Request<ResolveNodeRequest>,
    ) -> std::result::Result<Response<ResolveNodeResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(ResolveNodeResponse {
            node: self.address_resolver.find(request.node_id),
        }))
    }

    async fn report(
        &self,
        request: Request<ReportRequest>,
    ) -> std::result::Result<Response<ReportResponse>, Status> {
        let request = request.into_inner();
        let schema = self.schema().await?;
        for u in request.updates {
            if u.group_desc.is_some() {
                // TODO: check & handle remove replicas from group
            }
            schema
                .update_group_replica(u.group_desc, u.replica_state)
                .await?;
        }
        Ok(Response::new(ReportResponse {}))
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
            .schema()
            .await?
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
        self.schema().await?.delete_database(&req.name).await?;
        Ok(DeleteDatabaseResponse {})
    }

    async fn handle_get_database(&self, req: GetDatabaseRequest) -> Result<GetDatabaseResponse> {
        let resp = self.schema().await?.get_database(&req.name).await?;
        Ok(GetDatabaseResponse { database: resp })
    }

    async fn handle_create_collection(
        &self,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        let schema = self.schema().await?;
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
        let schema = self.schema().await?;
        let collection = schema.get_collection(&req.parent, &req.name).await?;
        if let Some(collection) = collection {
            schema.delete_collection(collection).await?;
            return Ok(DeleteCollectionResponse {});
        }
        Ok(DeleteCollectionResponse {})
    }

    async fn handle_get_collection(
        &self,
        req: GetCollectionRequest,
    ) -> Result<GetCollectionResponse> {
        let resp = self
            .schema()
            .await?
            .get_collection(&req.parent, &req.name)
            .await?;
        Ok(GetCollectionResponse { collection: resp })
    }

    async fn schema(&self) -> Result<Arc<Schema>> {
        let s = self.root.schema();
        if s.is_none() {
            let roots = self.node.get_root().await;
            return Err(Error::NotRootLeader(roots));
        }
        Ok(s.unwrap())
    }
}

pub(crate) struct ListStream {
    seq: u64,
    updates: Vec<UpdateEvent>,
}

impl ListStream {
    pub(crate) fn into_stream(self) -> WatchStream {
        Box::pin(async_stream::stream! {
                yield Ok(WatchResponse {
                        sequence: self.seq,
                        updates: self.updates,
                        deletes: vec![],
                })
        })
    }
}
