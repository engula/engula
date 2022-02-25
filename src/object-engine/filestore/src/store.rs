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

use crate::{async_trait, Result};

#[async_trait]
pub trait Store {
    type Tenant: Tenant;

    fn tenant(&self, name: &str) -> Self::Tenant;

    async fn create_tenant(&self, name: &str) -> Result<Self::Tenant>;
}

#[async_trait]
pub trait Tenant {
    type Bucket: Bucket;

    fn bucket(&self, name: &str) -> Self::Bucket;

    async fn create_bucket(&self, name: &str) -> Result<Self::Bucket>;
}

#[async_trait]
pub trait Bucket {
    type RandomReader: RandomRead;
    type SequentialWriter: SequentialWrite;

    async fn new_random_reader(&self, name: &str) -> Result<Self::RandomReader>;

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter>;
}

#[async_trait]
pub trait RandomRead {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}

#[async_trait]
pub trait SequentialWrite {
    async fn write(&mut self, buf: &[u8]) -> Result<()>;

    async fn flush(&mut self) -> Result<()>;
}
