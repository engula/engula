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

use engula_futures::{
    io::{RandomRead, SequentialWrite},
    stream::BatchResultStream,
};

use crate::{async_trait, Error, Result};

/// An interface to manipulate a storage.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    type BucketLister: BatchResultStream<Elem = String, Error = Error>;
    type ObjectLister: BatchResultStream<Elem = String, Error = Error>;
    type RandomReader: RandomRead + Send + Unpin;
    type SequentialWriter: SequentialWrite + Send + Unpin;

    /// Lists buckets.
    async fn list_buckets(&self) -> Result<Self::BucketLister>;

    /// Creates a bucket.
    ///
    /// # Errors
    ///
    /// Returns `Error::AlreadyExists` if the bucket already exists.
    async fn create_bucket(&self, bucket_name: &str) -> Result<()>;

    /// Deletes a bucket.
    ///
    /// The behavior of using a deleted bucket depends on the implementation.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the bucket doesn't exist.
    async fn delete_bucket(&self, bucket_name: &str) -> Result<()>;

    /// Lists objects in the given bucket.
    async fn list_objects(&self, bucket_name: &str) -> Result<Self::ObjectLister>;

    /// Deletes an object from the given bucket.
    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()>;

    /// Returns an object reader for random reads.
    async fn new_random_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader>;

    /// Returns an object writer for sequential writes.
    async fn new_sequential_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialWriter>;
}
