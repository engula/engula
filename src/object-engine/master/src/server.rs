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

use tonic::{Request, Response, Status};

use crate::{proto::*, Error, Master, Result, Tenant};

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

    async fn bucket(&self, req: Request<BucketRequest>) -> TonicResult<Response<BucketResponse>> {
        let req = req.into_inner();
        let res = self.handle_bucket(req).await?;
        Ok(Response::new(res))
    }

    async fn ingest(&self, _: Request<IngestRequest>) -> TonicResult<Response<IngestResponse>> {
        todo!()
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
        let req = req
            .request
            .ok_or_else(|| Error::invalid_argument("missing tenant request"))?;
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
            tenant_request_union::Request::LookupTenant(req) => {
                let res = self.handle_lookup_tenant(req).await?;
                tenant_response_union::Response::LookupTenant(res)
            }
        };
        Ok(TenantResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        let desc = req
            .desc
            .ok_or_else(|| Error::invalid_argument("missing tenant descriptor"))?;
        let desc = self.master.create_tenant(desc).await?;
        Ok(CreateTenantResponse { desc: Some(desc) })
    }

    async fn handle_lookup_tenant(&self, req: LookupTenantRequest) -> Result<LookupTenantResponse> {
        let db = self.master.tenant(&req.name).await?;
        let desc = db.desc().await;
        Ok(LookupTenantResponse { desc: Some(desc) })
    }
}

impl Server {
    async fn handle_bucket(&self, req: BucketRequest) -> Result<BucketResponse> {
        let tenant = self.master.tenant(&req.tenant).await?;
        let mut res = BucketResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_bucket_union(tenant.clone(), req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_bucket_union(
        &self,
        tenant: Tenant,
        req: BucketRequestUnion,
    ) -> Result<BucketResponseUnion> {
        let req = req
            .request
            .ok_or_else(|| Error::invalid_argument("missing bucket request"))?;
        let res = match req {
            bucket_request_union::Request::ListBuckets(_req) => {
                todo!();
            }
            bucket_request_union::Request::CreateBucket(req) => {
                let res = self.handle_create_bucket(tenant, req).await?;
                bucket_response_union::Response::CreateBucket(res)
            }
            bucket_request_union::Request::UpdateBucket(_req) => {
                todo!();
            }
            bucket_request_union::Request::DeleteBucket(_req) => {
                todo!();
            }
            bucket_request_union::Request::LookupBucket(req) => {
                let res = self.handle_lookup_bucket(tenant, req).await?;
                bucket_response_union::Response::LookupBucket(res)
            }
        };
        Ok(BucketResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_bucket(
        &self,
        tenant: Tenant,
        req: CreateBucketRequest,
    ) -> Result<CreateBucketResponse> {
        let desc = req
            .desc
            .ok_or_else(|| Error::invalid_argument("missing bucket descriptor"))?;
        let desc = tenant.create_bucket(desc).await?;
        Ok(CreateBucketResponse { desc: Some(desc) })
    }

    async fn handle_lookup_bucket(
        &self,
        tenant: Tenant,
        req: LookupBucketRequest,
    ) -> Result<LookupBucketResponse> {
        let desc = tenant.bucket(&req.name).await?;
        Ok(LookupBucketResponse { desc: Some(desc) })
    }
}
