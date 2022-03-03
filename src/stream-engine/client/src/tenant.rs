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

use crate::{master::Tenant as TenantClient, Engine, Result, Stream};

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<TenantInner>,
}

impl Tenant {
    pub fn new(name: &str, engine: Engine) -> Self {
        let tenant_client = engine.master.tenant(name);
        Self::new_with_client(engine, tenant_client)
    }

    pub fn new_with_client(engine: Engine, tenant_client: TenantClient) -> Self {
        let inner = TenantInner {
            engine,
            tenant_client,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    #[inline(always)]
    pub async fn desc(&self) -> Result<TenantDesc> {
        self.inner.tenant_client.desc().await
    }

    #[inline(always)]
    pub async fn stream(&self, name: &str) -> Result<Stream> {
        let stream_client = self.inner.tenant_client.stream(name).await?;
        self.inner.engine.create_stream(stream_client).await
    }

    #[inline(always)]
    pub async fn create_stream(&self, name: &str) -> Result<Stream> {
        let stream_client = self.inner.tenant_client.create_stream(name).await?;
        self.inner.engine.create_stream(stream_client).await
    }

    #[inline(always)]
    pub async fn delete_stream(&self, name: &str) -> Result<()> {
        self.inner.tenant_client.delete_stream(name).await
    }
}

struct TenantInner {
    engine: Engine,
    tenant_client: TenantClient,
}
