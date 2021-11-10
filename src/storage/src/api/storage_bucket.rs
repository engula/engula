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

use super::{async_trait, error::Result, storage_object::StorageObject, Stream};

/// An interface to manipulate objects in a bucket.
#[async_trait]
pub trait StorageBucket {
    /// Returns a handle to the object.
    async fn object(&self, name: &str) -> Result<Box<dyn StorageObject>>;

    /// Returns a stream of object names.
    async fn list_objects(&self) -> Box<dyn Stream<Item = Result<Vec<String>>>>;

    /// Uploads an object.
    async fn upload_object(&self, name: &str) -> Box<dyn StorageObjectUploader>;

    /// Deletes an object.
    async fn delete_object(&self, name: &str) -> Result<()>;
}

/// An interface to upload an object.
#[async_trait]
pub trait StorageObjectUploader {
    /// Writes some bytes.
    async fn write(&mut self, buf: &[u8]);

    /// Finishes this upload.
    async fn finish(self) -> Result<usize>;
}
