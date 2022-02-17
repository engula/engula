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

use object_engine_proto::*;

use crate::{master::Master, Error, Result};

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<TenantInner>,
}

impl Tenant {
    pub fn new(name: String, master: Master) -> Self {
        let inner = TenantInner { name, master };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn desc(&self) -> Result<TenantDesc> {
        let req = DescribeTenantRequest {
            name: self.inner.name.clone(),
        };
        let req = tenant_request_union::Request::DescribeTenant(req);
        let res = self.inner.tenant_union_call(req).await?;
        let desc = if let tenant_response_union::Response::DescribeTenant(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or(Error::InvalidResponse)
    }

    pub async fn create_bucket(&self, name: &str) -> Result<()> {
        let desc = BucketDesc {
            name: name.to_owned(),
            ..Default::default()
        };
        let req = CreateBucketRequest { desc: Some(desc) };
        let req = bucket_request_union::Request::CreateBucket(req);
        self.inner.bucket_union_call(req).await?;
        Ok(())
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        let req = DeleteBucketRequest {
            name: name.to_owned(),
        };
        let req = bucket_request_union::Request::DeleteBucket(req);
        self.inner.bucket_union_call(req).await?;
        Ok(())
    }
}

struct TenantInner {
    name: String,
    master: Master,
}

impl TenantInner {
    async fn tenant_union_call(
        &self,
        req: tenant_request_union::Request,
    ) -> Result<tenant_response_union::Response> {
        self.master.tenant_union(req).await
    }

    async fn bucket_union_call(
        &self,
        req: bucket_request_union::Request,
    ) -> Result<bucket_response_union::Response> {
        self.master.bucket_union(self.name.clone(), req).await
    }
}
