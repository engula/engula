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

use object_engine_master::proto::*;
use tonic::transport::Channel;

use crate::{Error, Result};

#[derive(Clone)]
pub struct Master {
    client: master_client::MasterClient<Channel>,
}

impl Master {
    pub fn new(chan: Channel) -> Self {
        let client = master_client::MasterClient::new(chan);
        Self { client }
    }

    pub async fn tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        let res = self.client.clone().tenant(req).await?;
        Ok(res.into_inner())
    }

    pub async fn tenant_union(
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
            .ok_or(Error::InvalidResponse)
    }

    pub async fn bucket(&self, req: BucketRequest) -> Result<BucketResponse> {
        let res = self.client.clone().bucket(req).await?;
        Ok(res.into_inner())
    }

    pub async fn bucket_union(
        &self,
        tenant: String,
        req: bucket_request_union::Request,
    ) -> Result<bucket_response_union::Response> {
        let req = BucketRequest {
            tenant,
            requests: vec![BucketRequestUnion { request: Some(req) }],
        };
        let mut res = self.bucket(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or(Error::InvalidResponse)
    }
}
