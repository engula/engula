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

use tonic::transport::Channel;

use crate::{proto::*, Error, Result};

#[derive(Clone)]
pub struct Master {
    client: master_client::MasterClient<Channel>,
}

impl Master {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let client = master_client::MasterClient::connect(url.into()).await?;
        Ok(Self { client })
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

    pub async fn lookup_tenant(&self, name: String) -> Result<TenantDesc> {
        let req = LookupTenantRequest { name };
        let req = tenant_request_union::Request::LookupTenant(req);
        let res = self.tenant_union(req).await?;
        let desc = if let tenant_response_union::Response::LookupTenant(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing tenant descriptor"))
    }

    pub async fn create_bucket(&self, tenant_id: u64, desc: BucketDesc) -> Result<BucketDesc> {
        let req = CreateBucketRequest { desc: Some(desc) };
        let req = bucket_request_union::Request::CreateBucket(req);
        let res = self.bucket_union(tenant_id, req).await?;
        let desc = if let bucket_response_union::Response::CreateBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing bucket descriptor"))
    }

    pub async fn lookup_bucket(&self, tenant_id: u64, name: String) -> Result<BucketDesc> {
        let req = LookupBucketRequest { name };
        let req = bucket_request_union::Request::LookupBucket(req);
        let res = self.bucket_union(tenant_id, req).await?;
        let desc = if let bucket_response_union::Response::LookupBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing bucket descriptor"))
    }
}

impl Master {
    async fn tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        let res = self.client.clone().tenant(req).await?;
        Ok(res.into_inner())
    }

    async fn tenant_union(
        &self,
        req: tenant_request_union::Request,
    ) -> Result<tenant_response_union::Response> {
        let req = TenantRequest {
            requests: vec![TenantRequestUnion { request: Some(req) }],
        };
        let mut res = self.tenant(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing tenant response"))
    }

    pub async fn bucket(&self, req: BucketRequest) -> Result<BucketResponse> {
        let res = self.client.clone().bucket(req).await?;
        Ok(res.into_inner())
    }

    pub async fn bucket_union(
        &self,
        tenant_id: u64,
        req: bucket_request_union::Request,
    ) -> Result<bucket_response_union::Response> {
        let req = BucketRequest {
            tenant_id,
            requests: vec![BucketRequestUnion { request: Some(req) }],
        };
        let mut res = self.bucket(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing bucket response"))
    }
}
