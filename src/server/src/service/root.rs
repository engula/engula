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
use tonic::{Request, Response, Status};

use crate::{root::Watcher, Error, Result, Server};

#[tonic::async_trait]
impl root_server::Root for Server {
    type WatchStream = Watcher;

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
        let watcher = self
            .wrap(self.root.watch(req.cur_group_epochs).await)
            .await?;
        Ok(Response::new(watcher))
    }

    async fn join(
        &self,
        request: Request<JoinNodeRequest>,
    ) -> std::result::Result<Response<JoinNodeResponse>, Status> {
        let request = request.into_inner();
        let (cluster_id, node, roots) = self.wrap(self.root.join(request.addr).await).await?;
        self.address_resolver.insert(&node);
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
        self.wrap(self.root.report(request.updates).await).await?;
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
        let desc = self.wrap(self.root.create_database(req.name).await).await?;
        Ok(CreateDatabaseResponse {
            database: Some(desc),
        })
    }

    async fn handle_delete_database(
        &self,
        req: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse> {
        self.wrap(self.root.delete_database(&req.name).await)
            .await?;
        Ok(DeleteDatabaseResponse {})
    }

    async fn handle_get_database(&self, req: GetDatabaseRequest) -> Result<GetDatabaseResponse> {
        let database = self.wrap(self.root.get_database(&req.name).await).await?;
        Ok(GetDatabaseResponse { database })
    }

    async fn handle_create_collection(
        &self,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        let desc = self
            .wrap(self.root.create_collection(req.name, req.parent).await)
            .await?;
        Ok(CreateCollectionResponse {
            collection: Some(desc),
        })
    }

    async fn handle_delete_collection(
        &self,
        req: DeleteCollectionRequest,
    ) -> Result<DeleteCollectionResponse> {
        self.wrap(self.root.delete_collection(&req.name, &req.parent).await)
            .await?;
        Ok(DeleteCollectionResponse {})
    }

    async fn handle_get_collection(
        &self,
        req: GetCollectionRequest,
    ) -> Result<GetCollectionResponse> {
        let collection = self
            .wrap(self.root.get_collection(&req.name, &req.parent).await)
            .await?;
        Ok(GetCollectionResponse { collection })
    }

    async fn wrap<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(Error::NotRootLeader(_)) = result {
            let roots = self.node.get_root().await;
            return Err(Error::NotRootLeader(roots));
        }
        result
    }
}
