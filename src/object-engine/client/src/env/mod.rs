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

use crate::{async_trait, Error, Result};

mod local;
mod remote;

pub use self::{local::Env as LocalEnv, remote::Env as RemoteEnv};

#[async_trait]
pub trait Env: Clone + Sync + Send {
    type TenantEnv: TenantEnv;

    async fn tenant(&self, name: &str) -> Result<Self::TenantEnv>;

    async fn handle_batch(&self, req: BatchRequest) -> Result<BatchResponse>;

    async fn handle_union(&self, req: request_union::Request) -> Result<response_union::Response> {
        let req = BatchRequest {
            requests: vec![RequestUnion { request: Some(req) }],
        };
        let mut res = self.handle_batch(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing response"))
    }
}

#[async_trait]
pub trait TenantEnv: Clone + Sync + Send {
    type BucketEnv: BucketEnv;

    fn name(&self) -> &str;

    async fn bucket(&self, name: &str) -> Result<Self::BucketEnv>;
}

#[async_trait]
pub trait BucketEnv: Clone + Sync + Send {
    fn name(&self) -> &str;

    fn tenant(&self) -> &str;

    async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn SequentialWrite>>;

    async fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>>;

    async fn iter(&self) -> Result<Box<dyn Iter>>;
}

#[async_trait]
pub trait Iter {
    fn key(&self) -> Vec<u8>;

    fn value(&self) -> &[u8];

    fn valid(&self) -> bool;

    async fn seek_to_first(&mut self) -> Result<()>;

    async fn seek(&mut self, target: &[u8]) -> Result<()>;

    async fn next(&mut self) -> Result<()>;
}
