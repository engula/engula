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

use object_engine_filestore::SequentialWrite;
use object_engine_master::proto::*;

use crate::{async_trait, Result};

type Client = master_client::MasterClient<tonic::transport::Channel>;

#[derive(Clone)]
pub struct Env {
    client: Client,
}

impl Env {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let client = Client::connect(url.into()).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl super::Env for Env {
    type TenantEnv = TenantEnv;

    async fn tenant(&self, name: &str) -> Result<Self::TenantEnv> {
        Ok(TenantEnv::new(name.to_owned()))
    }

    async fn handle_batch(&self, req: BatchRequest) -> Result<BatchResponse> {
        let res = self.client.clone().batch(req).await?;
        Ok(res.into_inner())
    }
}

#[derive(Clone)]
pub struct TenantEnv {
    name: String,
}

impl TenantEnv {
    fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl super::TenantEnv for TenantEnv {
    type BucketEnv = BucketEnv;

    fn name(&self) -> &str {
        &self.name
    }

    async fn bucket(&self, name: &str) -> Result<Self::BucketEnv> {
        Ok(BucketEnv::new(name.to_owned(), self.name.clone()))
    }
}

#[derive(Clone)]
pub struct BucketEnv {
    name: String,
    tenant: String,
}

impl BucketEnv {
    fn new(name: String, tenant: String) -> Self {
        Self { name, tenant }
    }
}

#[async_trait]
impl super::BucketEnv for BucketEnv {
    fn name(&self) -> &str {
        &self.name
    }

    fn tenant(&self) -> &str {
        &self.tenant
    }

    async fn new_sequential_writer(&self, _name: &str) -> Result<Box<dyn SequentialWrite>> {
        todo!();
    }
}
