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

    async fn list_tenants(&self) -> Result<Box<dyn Lister<Item = String>>>;

    async fn create_tenant(&self, name: &str) -> Result<Box<dyn Tenant>>;

    async fn delete_tenant(&self, name: &str) -> Result<()>;
}

#[async_trait]
pub trait Lister {
    type Item;

    /// Returns the next `n` items.
    async fn next(&mut self, n: usize) -> Result<Vec<Self::Item>>;
}

#[async_trait]
pub trait Tenant: Send + Sync {
    fn bucket(&self, name: &str) -> Box<dyn Bucket>;

    async fn list_buckets(&self) -> Result<Box<dyn Lister<Item = String>>>;

    async fn create_bucket(&self, name: &str) -> Result<Box<dyn Bucket>>;

    async fn delete_bucket(&self, name: &str) -> Result<()>;
}

#[async_trait]
pub trait Bucket: Send + Sync {
    async fn list_files(&self) -> Result<Box<dyn Lister<Item = FileDesc>>>;

    async fn new_random_reader(&self, name: &str) -> Result<Box<dyn RandomRead>>;

    async fn new_sequential_reader(&self, name: &str) -> Result<Box<dyn SequentialRead>>;

    async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn SequentialWrite>>;
}

pub struct FileDesc {
    pub name: String,
    pub size: usize,
}

#[async_trait]
pub trait RandomRead: Send + Sync {
    async fn read_at(&self, buf: &mut [u8], offset: usize) -> Result<usize>;

    async fn read_exact_at(&self, buf: &mut [u8], offset: usize) -> Result<()>;
}

#[async_trait]
pub trait SequentialRead: Send {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()>;
}

#[async_trait]
pub trait SequentialWrite: Send {
    async fn write(&mut self, buf: &[u8]) -> Result<()>;

    async fn finish(&mut self) -> Result<()>;
}
