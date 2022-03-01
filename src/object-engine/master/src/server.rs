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

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{proto::*, Error, Result};

pub struct Server {
    inner: Arc<Mutex<Inner>>,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    pub fn into_service(self) -> master_server::MasterServer<Self> {
        master_server::MasterServer::new(self)
    }

    async fn tenant(&self, id: u64) -> Result<Tenant> {
        let inner = self.inner.lock().await;
        inner.tenant(id)
    }

    async fn lookup_tenant(&self, name: &str) -> Result<Tenant> {
        let inner = self.inner.lock().await;
        inner.lookup_tenant(name)
    }

    async fn create_tenant(&self, desc: TenantDesc) -> Result<TenantDesc> {
        let mut inner = self.inner.lock().await;
        inner.create_tenant(desc)
    }
}

type TonicResult<T> = std::result::Result<T, Status>;

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
    async fn handle_tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        let mut res = TenantResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_tenant_union(req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_tenant_union(&self, req: TenantRequestUnion) -> Result<TenantResponseUnion> {
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
        let desc = self.create_tenant(desc).await?;
        Ok(CreateTenantResponse { desc: Some(desc) })
    }

    async fn handle_lookup_tenant(&self, req: LookupTenantRequest) -> Result<LookupTenantResponse> {
        let tenant = self.lookup_tenant(&req.name).await?;
        let desc = tenant.desc().await;
        Ok(LookupTenantResponse { desc: Some(desc) })
    }
}

impl Server {
    async fn handle_bucket(&self, req: BucketRequest) -> Result<BucketResponse> {
        let tenant = self.tenant(req.tenant_id).await?;
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
        let desc = tenant.lookup_bucket(&req.name).await?;
        Ok(LookupBucketResponse { desc: Some(desc) })
    }
}

struct Inner {
    next_id: u64,
    tenants: HashMap<u64, Tenant>,
    tenant_names: HashMap<String, u64>,
}

impl Inner {
    fn new() -> Self {
        Inner {
            next_id: 1,
            tenants: HashMap::new(),
            tenant_names: HashMap::new(),
        }
    }

    fn tenant(&self, id: u64) -> Result<Tenant> {
        self.tenants
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("tenant id {}", id)))
    }

    fn lookup_tenant(&self, name: &str) -> Result<Tenant> {
        let id = self
            .tenant_names
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("tenant name {}", name)))?;
        self.tenant(id)
    }

    fn create_tenant(&mut self, mut desc: TenantDesc) -> Result<TenantDesc> {
        if self.tenant_names.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("tenant name {}", desc.name)));
        }
        desc.id = self.next_id;
        self.next_id += 1;
        let tenant = Tenant::new(desc.clone());
        self.tenants.insert(desc.id, tenant);
        self.tenant_names.insert(desc.name.clone(), desc.id);
        Ok(desc)
    }
}

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<Mutex<TenantInner>>,
}

impl Tenant {
    fn new(desc: TenantDesc) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TenantInner::new(desc))),
        }
    }

    async fn desc(&self) -> TenantDesc {
        self.inner.lock().await.desc()
    }

    pub async fn lookup_bucket(&self, name: &str) -> Result<BucketDesc> {
        let inner = self.inner.lock().await;
        inner.lookup_bucket(name)
    }

    pub async fn create_bucket(&self, desc: BucketDesc) -> Result<BucketDesc> {
        let mut inner = self.inner.lock().await;
        inner.create_bucket(desc)
    }
}

struct TenantInner {
    desc: TenantDesc,
    next_id: u64,
    buckets: HashMap<u64, BucketDesc>,
    bucket_names: HashMap<String, u64>,
}

impl TenantInner {
    fn new(desc: TenantDesc) -> Self {
        Self {
            desc,
            next_id: 1,
            buckets: HashMap::new(),
            bucket_names: HashMap::new(),
        }
    }

    fn desc(&self) -> TenantDesc {
        self.desc.clone()
    }

    fn bucket(&self, id: u64) -> Result<BucketDesc> {
        self.buckets
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("bucket id {}", id)))
    }

    fn lookup_bucket(&self, name: &str) -> Result<BucketDesc> {
        let id = self
            .bucket_names
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("bucket name {}", name)))?;
        self.bucket(id)
    }

    fn create_bucket(&mut self, mut desc: BucketDesc) -> Result<BucketDesc> {
        if self.bucket_names.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("bucket name {}", desc.name)));
        }
        desc.id = self.next_id;
        self.next_id += 1;
        desc.parent_id = self.desc.id;
        self.buckets.insert(desc.id, desc.clone());
        self.bucket_names.insert(desc.name.clone(), desc.id);
        Ok(desc)
    }
}
