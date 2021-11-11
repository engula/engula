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

use super::{async_trait, bucket::Bucket, error::Result, ResultStream};

/// An interface to manipulate buckets.
#[async_trait]
pub trait Storage {
    /// Returns a handle to the bucket.
    async fn bucket(&self, name: &str) -> Result<Box<dyn Bucket>>;

    /// Returns a stream of bucket names.
    async fn list_buckets(&self) -> ResultStream<String>;

    /// Creates a bucket.
    async fn create_bucket(&self, name: &str) -> Result<Box<dyn Bucket>>;

    /// Deletes a bucket.
    async fn delete_bucket(&self, name: &str) -> Result<()>;
}
