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

use stream_engine_proto::*;
use tonic::{Request, Response, Status};

use crate::{
    error::{Error, Result},
    master::{Master, Tenant},
};

type TonicResult<T> = std::result::Result<T, Status>;

pub struct Server {
    master: Master,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self {
            master: Master::new(),
        }
    }

    pub fn into_service(self) -> master_server::MasterServer<Self> {
        master_server::MasterServer::new(self)
    }
}

#[tonic::async_trait]
impl master_server::Master for Server {
    async fn tenant(&self, req: Request<TenantRequest>) -> TonicResult<Response<TenantResponse>> {
        let req = req.into_inner();
        let res = self.handle_tenant(req).await?;
        Ok(Response::new(res))
    }

    async fn stream(&self, req: Request<StreamRequest>) -> TonicResult<Response<StreamResponse>> {
        let req = req.into_inner();
        let res = self.handle_stream(req).await?;
        Ok(Response::new(res))
    }
}

impl Server {
    pub(crate) async fn handle_tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        let mut res = TenantResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_tenant_union(req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    pub(crate) async fn handle_tenant_union(
        &self,
        req: TenantRequestUnion,
    ) -> Result<TenantResponseUnion> {
        let req = req.request.ok_or(Error::InvalidRequest)?;
        let res = match req {
            tenant_request_union::Request::ListTenants(_req) => {
                todo!();
            }
            tenant_request_union::Request::CreateTenant(req) => {
                let res = self.handle_create_tenant(req).await?;
                tenant_response_union::Response::CreateTenant(res)
            }
            tenant_request_union::Request::UpdateTenant(_req) => {
                todo!();
            }
            tenant_request_union::Request::DeleteTenant(_req) => {
                todo!();
            }
            tenant_request_union::Request::DescribeTenant(req) => {
                let res = self.handle_describe_tenant(req).await?;
                tenant_response_union::Response::DescribeTenant(res)
            }
        };
        Ok(TenantResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        let desc = req.desc.ok_or(Error::InvalidRequest)?;
        let desc = self.master.create_tenant(desc).await?;
        Ok(CreateTenantResponse { desc: Some(desc) })
    }

    async fn handle_describe_tenant(
        &self,
        req: DescribeTenantRequest,
    ) -> Result<DescribeTenantResponse> {
        let db = self.master.tenant(&req.name).await?;
        let desc = db.desc().await;
        Ok(DescribeTenantResponse { desc: Some(desc) })
    }
}

impl Server {
    async fn handle_stream(&self, req: StreamRequest) -> Result<StreamResponse> {
        let tenant = self.master.tenant(&req.tenant).await?;
        let mut res = StreamResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_stream_union(tenant.clone(), req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_stream_union(
        &self,
        tenant: Tenant,
        req: StreamRequestUnion,
    ) -> Result<StreamResponseUnion> {
        let req = req.request.ok_or(Error::InvalidRequest)?;
        let res = match req {
            stream_request_union::Request::ListStreams(_req) => {
                todo!();
            }
            stream_request_union::Request::CreateStream(req) => {
                let res = self.handle_create_stream(tenant, req).await?;
                stream_response_union::Response::CreateStream(res)
            }
            stream_request_union::Request::UpdateStream(_req) => {
                todo!();
            }
            stream_request_union::Request::DeleteStream(_req) => {
                todo!();
            }
            stream_request_union::Request::DescribeStream(req) => {
                let res = self.handle_describe_stream(tenant, req).await?;
                stream_response_union::Response::DescribeStream(res)
            }
        };
        Ok(StreamResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_stream(
        &self,
        tenant: Tenant,
        req: CreateStreamRequest,
    ) -> Result<CreateStreamResponse> {
        let desc = req.desc.ok_or(Error::InvalidRequest)?;
        let desc = tenant.create_stream(desc).await?;
        Ok(CreateStreamResponse { desc: Some(desc) })
    }

    async fn handle_describe_stream(
        &self,
        tenant: Tenant,
        req: DescribeStreamRequest,
    ) -> Result<DescribeStreamResponse> {
        let desc = tenant.stream(&req.name).await?;
        Ok(DescribeStreamResponse { desc: Some(desc) })
    }
}
