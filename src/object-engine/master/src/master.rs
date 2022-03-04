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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use tokio::sync::Mutex;

use crate::{fs, proto::*, Error, FileBucket, FileStore, FileTenant, Result};

#[derive(Clone)]
pub struct Master {
    inner: Arc<Mutex<MasterInner>>,
}

impl Master {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let file_store = fs::open(path).await?;
        let inner = MasterInner::new(file_store);
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    async fn tenant(&self, name: &str) -> Result<Tenant> {
        let inner = self.inner.lock().await;
        inner.tenant(name)
    }

    async fn create_tenant(&self, desc: TenantDesc) -> Result<TenantDesc> {
        let mut inner = self.inner.lock().await;
        inner.create_tenant(desc).await
    }
}

impl Master {
    pub async fn handle_tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
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
            tenant_request_union::Request::GetTenant(req) => {
                let res = self.handle_get_tenant(req).await?;
                tenant_response_union::Response::GetTenant(res)
            }
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
        };
        Ok(TenantResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_get_tenant(&self, req: GetTenantRequest) -> Result<GetTenantResponse> {
        let tenant = self.tenant(&req.name).await?;
        let desc = tenant.desc().await;
        Ok(GetTenantResponse { desc: Some(desc) })
    }

    async fn handle_create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        let desc = req
            .desc
            .ok_or_else(|| Error::invalid_argument("missing tenant descriptor"))?;
        let desc = self.create_tenant(desc).await?;
        Ok(CreateTenantResponse { desc: Some(desc) })
    }
}

impl Master {
    pub async fn handle_bucket(&self, req: BucketRequest) -> Result<BucketResponse> {
        let tenant = self.tenant(&req.tenant).await?;
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
            bucket_request_union::Request::GetBucket(req) => {
                let res = self.handle_get_bucket(tenant, req).await?;
                bucket_response_union::Response::GetBucket(res)
            }
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
        };
        Ok(BucketResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_get_bucket(
        &self,
        tenant: Tenant,
        req: GetBucketRequest,
    ) -> Result<GetBucketResponse> {
        let bucket = tenant.bucket(&req.name).await?;
        let desc = bucket.desc().await;
        Ok(GetBucketResponse { desc: Some(desc) })
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
}

impl Master {
    pub async fn handle_engine(&self, _: EngineRequest) -> Result<EngineResponse> {
        todo!();
    }
}

struct MasterInner {
    tenants: HashMap<String, Tenant>,
    file_store: FileStore,
}

impl MasterInner {
    fn new(file_store: FileStore) -> Self {
        Self {
            tenants: HashMap::new(),
            file_store,
        }
    }

    fn tenant(&self, name: &str) -> Result<Tenant> {
        self.tenants
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("tenant {}", name)))
    }

    async fn create_tenant(&mut self, desc: TenantDesc) -> Result<TenantDesc> {
        if self.tenants.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("tenant {}", desc.name)));
        }
        let file_tenant = self.file_store.create_tenant(&desc.name).await?;
        let tenant = Tenant::new(desc.clone(), file_tenant.into());
        self.tenants.insert(desc.name.clone(), tenant);
        Ok(desc)
    }
}

#[derive(Clone)]
struct Tenant {
    inner: Arc<Mutex<TenantInner>>,
}

impl Tenant {
    fn new(desc: TenantDesc, file_tenant: FileTenant) -> Self {
        let inner = TenantInner::new(desc, file_tenant);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    async fn desc(&self) -> TenantDesc {
        self.inner.lock().await.desc.clone()
    }

    async fn bucket(&self, name: &str) -> Result<Bucket> {
        let inner = self.inner.lock().await;
        inner.bucket(name)
    }

    async fn create_bucket(&self, desc: BucketDesc) -> Result<BucketDesc> {
        let mut inner = self.inner.lock().await;
        inner.create_bucket(desc).await
    }
}

struct TenantInner {
    desc: TenantDesc,
    buckets: HashMap<String, Bucket>,
    file_tenant: FileTenant,
}

impl TenantInner {
    fn new(desc: TenantDesc, file_tenant: FileTenant) -> Self {
        Self {
            desc,
            buckets: HashMap::new(),
            file_tenant,
        }
    }

    fn bucket(&self, name: &str) -> Result<Bucket> {
        self.buckets
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("bucket {}", name)))
    }

    async fn create_bucket(&mut self, mut desc: BucketDesc) -> Result<BucketDesc> {
        if self.buckets.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("bucket {}", desc.name)));
        }
        desc.tenant = self.desc.name.clone();
        let file_bucket = self.file_tenant.create_bucket(&desc.name).await?;
        let bucket = Bucket::new(desc.clone(), file_bucket.into());
        self.buckets.insert(desc.name.clone(), bucket);
        Ok(desc)
    }
}

#[derive(Clone)]
struct Bucket {
    inner: Arc<Mutex<BucketInner>>,
}

impl Bucket {
    fn new(desc: BucketDesc, file_bucket: FileBucket) -> Self {
        let inner = BucketInner::new(desc, file_bucket);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    async fn desc(&self) -> BucketDesc {
        self.inner.lock().await.desc.clone()
    }
}

#[allow(dead_code)]
struct BucketInner {
    desc: BucketDesc,
    file_bucket: FileBucket,
}

impl BucketInner {
    fn new(desc: BucketDesc, file_bucket: FileBucket) -> Self {
        Self { desc, file_bucket }
    }
}
