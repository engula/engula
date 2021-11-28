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

use crate::{async_trait, Object, ObjectUploader, Result};

/// An interface to manipulate a storage.
#[async_trait]
pub trait Storage {
    /// Creates a bucket.
    async fn create_bucket(&self, bucket_name: &str) -> Result<()>;

    /// Deletes a bucket.
    async fn delete_bucket(&self, bucket_name: &str) -> Result<()>;

    /// Returns an object.
    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<Box<dyn Object>>;

    /// Uploads an object.
    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Box<dyn ObjectUploader>>;

    /// Deletes an object.
    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()>;
}
