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

use stream_engine_proto::*;
use tonic::transport::Endpoint;

use crate::{master::Master, Error, Result, Tenant};

#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

impl Engine {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let chan = Endpoint::new(url.into())
            .map_err(Error::unknown)?
            .connect()
            .await
            .map_err(Error::unknown)?;
        let inner = EngineInner {
            master: Master::new(chan),
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub fn tenant(&self, name: &str) -> Tenant {
        self.inner.new_tenant(name.to_owned())
    }

    pub async fn create_tenant(&self, name: &str) -> Result<Tenant> {
        let desc = TenantDesc {
            name: name.to_owned(),
            ..Default::default()
        };
        let req = CreateTenantRequest { desc: Some(desc) };
        let req = tenant_request_union::Request::CreateTenant(req);
        self.inner.tenant_union_call(req).await?;
        Ok(self.tenant(name))
    }

    pub async fn delete_tenant(&self, name: &str) -> Result<()> {
        let req = DeleteTenantRequest {
            name: name.to_owned(),
        };
        let req = tenant_request_union::Request::DeleteTenant(req);
        self.inner.tenant_union_call(req).await?;
        Ok(())
    }
}

struct EngineInner {
    master: Master,
}

impl EngineInner {
    fn new_tenant(&self, name: String) -> Tenant {
        Tenant::new(name, self.master.clone())
    }

    async fn tenant_union_call(
        &self,
        req: tenant_request_union::Request,
    ) -> Result<tenant_response_union::Response> {
        self.master.tenant_union(req).await
    }
}
