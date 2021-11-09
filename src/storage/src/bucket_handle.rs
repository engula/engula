// Copyright 2021 The Engula Authors.
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

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    error::StorageResult,
    object_handle::{ObjectReader, ObjectWriter},
};

#[async_trait]
pub trait BucketHandle {
    async fn new_writer(&self, name: &str) -> StorageResult<Box<dyn ObjectWriter>>;

    async fn new_reader(&self, name: &str) -> Box<dyn ObjectReader>;

    async fn put_object(&self, name: &str, body: Bytes) -> StorageResult<()>;

    async fn delete_object(&self, name: &str) -> StorageResult<()>;

    async fn delete_objects(&self, key: Vec<String>) -> StorageResult<()>;
}
