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
pub trait Store: Send + Sync {
    fn tenant(&self, name: &str) -> Box<dyn Tenant>;

    async fn create_tenant(&self, name: &str) -> Result<Box<dyn Tenant>>;
}

#[async_trait]
pub trait Tenant: Send + Sync {
    async fn create_bucket(&self, bucket: &str) -> Result<()>;

    async fn new_random_reader(&self, bucket: &str, file_name: &str)
        -> Result<Box<dyn RandomRead>>;

    async fn new_sequential_writer(
        &self,
        bucket: &str,
        file_name: &str,
    ) -> Result<Box<dyn SequentialWrite>>;
}

#[async_trait]
pub trait RandomRead {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;
}

#[async_trait]
pub trait SequentialWrite {
    async fn write(&mut self, buf: &[u8]) -> Result<()>;

    async fn finish(&mut self) -> Result<()>;
}
