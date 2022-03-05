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

use std::path::PathBuf;

use object_engine_master::{proto::*, FileStore};

use crate::{Error, Result};

#[derive(Clone)]
pub struct Master {
    handle: MasterHandle,
}

impl Master {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let master = LocalMaster::open(path).await?;
        let handle = MasterHandle::Local(master);
        Ok(Self { handle })
    }

    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let master = RemoteMaster::connect(url.into()).await?;
        let handle = MasterHandle::Remote(master);
        Ok(Self { handle })
    }

    pub async fn get_tenant(&self, name: String) -> Result<TenantDesc> {
        let req = GetTenantRequest { name };
        let req = tenant_request_union::Request::GetTenant(req);
        let res = self.tenant_union(req).await?;
        let desc = if let tenant_response_union::Response::GetTenant(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing tenant descriptor"))
    }

    pub async fn create_tenant(&self, desc: TenantDesc) -> Result<TenantDesc> {
        let req = CreateTenantRequest { desc: Some(desc) };
        let req = tenant_request_union::Request::CreateTenant(req);
        let res = self.tenant_union(req).await?;
        let desc = if let tenant_response_union::Response::CreateTenant(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing tenant descriptor"))
    }

    pub async fn get_bucket(&self, tenant: String, name: String) -> Result<BucketDesc> {
        let req = GetBucketRequest { name };
        let req = bucket_request_union::Request::GetBucket(req);
        let res = self.bucket_union(tenant, req).await?;
        let desc = if let bucket_response_union::Response::GetBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing bucket descriptor"))
    }

    pub async fn create_bucket(&self, tenant: String, desc: BucketDesc) -> Result<BucketDesc> {
        let req = CreateBucketRequest { desc: Some(desc) };
        let req = bucket_request_union::Request::CreateBucket(req);
        let res = self.bucket_union(tenant, req).await?;
        let desc = if let bucket_response_union::Response::CreateBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing bucket descriptor"))
    }

    pub async fn file_store(&self) -> Result<FileStore> {
        self.handle.file_store().await
    }

    pub async fn begin_bulkload(&self, tenant: String) -> Result<String> {
        let req = BeginBulkLoadRequest {};
        let req = engine_request_union::Request::BeginBulkload(req);
        let res = self.engine_union(tenant, req).await?;
        if let engine_response_union::Response::BeginBulkload(res) = res {
            Ok(res.token)
        } else {
            Err(Error::internal("missing begin bulkload response"))
        }
    }

    pub async fn commit_bulkload(
        &self,
        tenant: String,
        bulkload: CommitBulkLoadRequest,
    ) -> Result<()> {
        let req = engine_request_union::Request::CommitBulkload(bulkload);
        let res = self.engine_union(tenant, req).await?;
        if let engine_response_union::Response::CommitBulkload(_) = res {
            Ok(())
        } else {
            Err(Error::internal("missing commit bulkload response"))
        }
    }

    #[allow(dead_code)]
    pub async fn allocate_file_names(
        &self,
        tenant: String,
        token: String,
        count: u64,
    ) -> Result<Vec<String>> {
        let req = AllocateFileNamesRequest { token, count };
        let req = engine_request_union::Request::AllocateFileNames(req);
        let res = self.engine_union(tenant, req).await?;
        if let engine_response_union::Response::AllocateFileNames(res) = res {
            Ok(res.names)
        } else {
            Err(Error::internal("missing allocate file names response"))
        }
    }
}

impl Master {
    async fn tenant_union(
        &self,
        req: tenant_request_union::Request,
    ) -> Result<tenant_response_union::Response> {
        let req = TenantRequest {
            requests: vec![TenantRequestUnion { request: Some(req) }],
        };
        let mut res = self.handle.tenant(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing tenant response"))
    }

    async fn bucket_union(
        &self,
        tenant: String,
        req: bucket_request_union::Request,
    ) -> Result<bucket_response_union::Response> {
        let req = BucketRequest {
            tenant,
            requests: vec![BucketRequestUnion { request: Some(req) }],
        };
        let mut res = self.handle.bucket(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing bucket response"))
    }

    async fn engine_union(
        &self,
        tenant: String,
        req: engine_request_union::Request,
    ) -> Result<engine_response_union::Response> {
        let req = EngineRequest {
            tenant,
            requests: vec![EngineRequestUnion { request: Some(req) }],
        };
        let mut res = self.handle.engine(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing engine response"))
    }
}

type LocalMaster = object_engine_master::Master;
type RemoteMaster = master_client::MasterClient<tonic::transport::Channel>;

#[derive(Clone)]
enum MasterHandle {
    Local(LocalMaster),
    Remote(RemoteMaster),
}

impl MasterHandle {
    async fn tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        match self {
            MasterHandle::Local(master) => master.handle_tenant(req).await,
            MasterHandle::Remote(client) => {
                let res = client.clone().tenant(req).await?;
                Ok(res.into_inner())
            }
        }
    }

    async fn bucket(&self, req: BucketRequest) -> Result<BucketResponse> {
        match self {
            MasterHandle::Local(master) => master.handle_bucket(req).await,
            MasterHandle::Remote(client) => {
                let res = client.clone().bucket(req).await?;
                Ok(res.into_inner())
            }
        }
    }

    async fn engine(&self, req: EngineRequest) -> Result<EngineResponse> {
        match self {
            MasterHandle::Local(master) => master.handle_engine(req).await,
            MasterHandle::Remote(client) => {
                let res = client.clone().engine(req).await?;
                Ok(res.into_inner())
            }
        }
    }

    async fn file_store(&self) -> Result<FileStore> {
        match self {
            MasterHandle::Local(master) => Ok(master.file_store().await),
            MasterHandle::Remote(_) => todo!(),
        }
    }
}
