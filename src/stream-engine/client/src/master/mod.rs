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

mod client;
mod stream;
mod tenant;

use stream_engine_proto::*;
use tonic::transport::Endpoint;

use self::client::MasterClient;
pub use self::{stream::Stream, tenant::Tenant};
use crate::Result;

#[derive(Clone)]
pub struct Master {
    master_client: MasterClient,
}

impl Master {
    pub async fn new(url: impl Into<String>) -> Result<Self> {
        let chan = Endpoint::new(url.into())?.connect().await?;
        Ok(Master {
            master_client: MasterClient::new(chan),
        })
    }

    pub fn tenant(&self, name: &str) -> Tenant {
        Tenant::new(name.to_owned(), self.master_client.clone())
    }

    pub async fn create_tenant(&self, name: &str) -> Result<Tenant> {
        let desc = TenantDesc {
            name: name.to_owned(),
            ..Default::default()
        };
        let req = CreateTenantRequest { desc: Some(desc) };
        let req = tenant_request_union::Request::CreateTenant(req);
        self.master_client.tenant_union(req).await?;
        Ok(self.tenant(name))
    }

    pub async fn delete_tenant(&self, name: &str) -> Result<()> {
        let req = DeleteTenantRequest {
            name: name.to_owned(),
        };
        let req = tenant_request_union::Request::DeleteTenant(req);
        self.master_client.tenant_union(req).await?;
        Ok(())
    }
}
