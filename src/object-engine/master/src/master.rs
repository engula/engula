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

use object_engine_lsmstore::Store;
use tokio::sync::Mutex;

use crate::{fs, proto::*, Error, Result, Tenant};

#[derive(Clone)]
pub struct Master {
    inner: Arc<MasterInner>,
}

impl Master {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file_store = fs::open(path.to_owned()).await?;
        let lsm_store = Store::new(path.to_owned(), None, file_store).await?;
        let inner = MasterInner::new(lsm_store);
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
            request_union::Request::BeginBulkload(req) => {
                let res = self.handle_begin_bulk_load(req).await?;
                response_union::Response::BeginBulkload(res)
            }
            request_union::Request::CommitBulkload(req) => {
                let res = self.handle_commit_bulk_load(req).await?;
                response_union::Response::CommitBulkload(res)
            }
            request_union::Request::AllocateFileNames(req) => {
                let res = self.handle_allocate_filenames(req).await?;
                response_union::Response::AllocateFileNames(res)
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

    async fn handle_begin_bulk_load(
        &self,
        req: BeginBulkLoadRequest,
    ) -> Result<BeginBulkLoadResponse> {
        let tenant = req.tenant;
        let uid = uuid::Uuid::new_v4().to_string();
        let token = tenant.to_owned() + &uid;
        let mut in_progress = self.inner.in_progress.lock().await;
        in_progress.insert(token.to_owned(), TokenCtx { tenant });
        Ok(BeginBulkLoadResponse { token })
    }

    async fn handle_commit_bulk_load(
        &self,
        req: CommitBulkLoadRequest,
    ) -> Result<CommitBulkLoadResponse> {
        let token = req.token;
        let tenant = self
            .inner
            .in_progress
            .lock()
            .await
            .get(&token)
            .ok_or_else(|| Error::NotFound("bulk load token".to_string()))?
            .tenant
            .to_owned();

        let mut files = Vec::new();
        for desc in req.files {
            let version_edit = object_engine_lsmstore::VersionEditFile {
                bucket: desc.bucket.to_owned(),
                lower_bound: desc.lower_bound.to_owned(),
                upper_bound: desc.upper_bound.to_owned(),
                name: desc.file_name.to_owned(),
                tenant: tenant.to_owned(),
                file_size: desc.file_size.to_owned(),
                range_id: 0,
                level: 0,
            };
            files.push(version_edit);
        }
        let tenant = self.tenant(&tenant).await?;
        tenant.add_files(files).await?;
        Ok(CommitBulkLoadResponse {})
    }

    async fn handle_allocate_filenames(
        &self,
        req: AllocateFileNamesRequest,
    ) -> Result<AllocateFileNamesResponse> {
        let token = req.token;
        let count = req.count;
        let tenant = self
            .inner
            .in_progress
            .lock()
            .await
            .get(&token)
            .ok_or_else(|| Error::NotFound("bulk load token".to_string()))?
            .tenant
            .to_owned();

        let mut names = Vec::with_capacity(count as usize);
        let file_nums = self.tenant(&tenant).await?.get_next_file_num(count).await?;
        for num in file_nums {
            names.push(format!("{:0>6}.sst", num));
        }

        Ok(AllocateFileNamesResponse { names })
    }
}

struct MasterInner {
    tenants: Mutex<HashMap<String, Tenant>>,
    in_progress: Mutex<HashMap<String, TokenCtx>>,
    store: Store,
}

struct TokenCtx {
    tenant: String,
}

impl MasterInner {
    fn new(store: Store) -> Self {
        Self {
            tenants: Mutex::new(HashMap::new()),
            in_progress: Mutex::new(HashMap::new()),
            store,
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
        self.store.create_tenant(name).await?;
        let versions_tenant = self.store.tenant(name).await?;

        let tenant = Tenant::new(name.to_owned(), options, versions_tenant);
        tenants.insert(name.to_owned(), tenant.clone());
        Ok(tenant)
    }
}
