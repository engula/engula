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

use engula_runtime::io::{RandomRead, SequentialWrite};

use crate::{async_trait, Result};

/// An interface to manipulate a storage.
#[async_trait]
pub trait Storage: Clone + Send + Sync + 'static {
    type RandomReader: RandomRead;
    type SequentialWriter: SequentialWrite;

    /// Creates a bucket.
    ///
    /// # Returns
    ///
    /// On failure, returns:
    ///
    /// - `Error::AlreadyExists` if the bucket already exists.
    async fn create_bucket(&self, bucket_name: &str) -> Result<()>;

    /// Deletes a bucket.
    ///
    /// Using a deleted bucket is an undefined behavior.
    ///
    /// # Returns
    ///
    /// On failure, returns:
    ///
    /// - `Error::NotFound` if the bucket doesn't exist.
    async fn delete_bucket(&self, bucket_name: &str) -> Result<()>;

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()>;

    async fn new_random_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader>;

    async fn new_sequential_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialWriter>;
}
