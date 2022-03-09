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

use crate::{
    fs::{self, FileStore},
    proto::*,
    Error, Result, Tenant,
};

#[derive(Clone)]
pub struct Master {
    inner: Arc<MasterInner>,
}

impl Master {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let file_store = fs::open(path).await?;
        let inner = MasterInner::new(file_store);
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub async fn tenant(&self, name: &str) -> Result<Tenant> {
        self.inner.tenant(name).await
    }
}

impl Master {
    pub async fn handle_batch(&self, req: BatchRequest) -> Result<BatchResponse> {
        let mut res = BatchResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_union(req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_union(&self, req: RequestUnion) -> Result<ResponseUnion> {
        let req = req
            .request
            .ok_or_else(|| Error::invalid_argument("missing request"))?;
        let res = match req {
            request_union::Request::ListTenants(_req) => {
                todo!();
            }
            request_union::Request::CreateTenant(req) => {
                let res = self.handle_create_tenant(req).await?;
                response_union::Response::CreateTenant(res)
            }
            request_union::Request::UpdateTenant(_req) => {
                todo!();
            }
            request_union::Request::DeleteTenant(_req) => {
                todo!();
            }
            request_union::Request::DescribeTenant(req) => {
                let res = self.handle_describe_tenant(req).await?;
                response_union::Response::DescribeTenant(res)
            }
            request_union::Request::ListBuckets(_req) => {
                todo!();
            }
            request_union::Request::CreateBucket(req) => {
                let res = self.handle_create_bucket(req).await?;
                response_union::Response::CreateBucket(res)
            }
            request_union::Request::UpdateBucket(_req) => {
                todo!();
            }
            request_union::Request::DeleteBucket(_req) => {
                todo!();
            }
            request_union::Request::DescribeBucket(req) => {
                let res = self.handle_describe_bucket(req).await?;
                response_union::Response::DescribeBucket(res)
            }
            _ => {
                todo!();
            }
        };
        Ok(ResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        let tenant = self
            .inner
            .create_tenant(&req.name, req.options.unwrap_or_default())
            .await?;
        Ok(CreateTenantResponse {
            desc: Some(tenant.desc().await),
        })
    }

    async fn handle_describe_tenant(
        &self,
        req: DescribeTenantRequest,
    ) -> Result<DescribeTenantResponse> {
        let tenant = self.tenant(&req.name).await?;
        Ok(DescribeTenantResponse {
            desc: Some(tenant.desc().await),
        })
    }

    async fn handle_create_bucket(&self, req: CreateBucketRequest) -> Result<CreateBucketResponse> {
        let tenant = self.tenant(&req.tenant).await?;
        let bucket = tenant
            .create_bucket(&req.bucket, req.options.unwrap_or_default())
            .await?;
        Ok(CreateBucketResponse {
            desc: Some(bucket.desc().await),
        })
    }

    async fn handle_describe_bucket(
        &self,
        req: DescribeBucketRequest,
    ) -> Result<DescribeBucketResponse> {
        let tenant = self.tenant(&req.tenant).await?;
        let bucket = tenant.bucket(&req.bucket).await?;
        Ok(DescribeBucketResponse {
            desc: Some(bucket.desc().await),
        })
    }
}

struct MasterInner {
    tenants: Mutex<HashMap<String, Tenant>>,
    file_store: FileStore,
}

impl MasterInner {
    fn new(file_store: FileStore) -> Self {
        Self {
            tenants: Mutex::new(HashMap::new()),
            file_store,
        }
    }

    async fn tenant(&self, name: &str) -> Result<Tenant> {
        let tenants = self.tenants.lock().await;
        tenants
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("tenant {}", name)))
    }

    async fn create_tenant(&self, name: &str, options: TenantOptions) -> Result<Tenant> {
        let mut tenants = self.tenants.lock().await;
        if tenants.contains_key(name) {
            return Err(Error::AlreadyExists(format!("tenant {}", name)));
        }
        let file_tenant = self.file_store.create_tenant(name).await?;
        let tenant = Tenant::new(name.to_owned(), options, file_tenant.into());
        tenants.insert(name.to_owned(), tenant.clone());
        Ok(tenant)
    }
}
