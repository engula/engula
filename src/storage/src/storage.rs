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

use super::{async_trait, bucket::Bucket, object::Object};

/// An interface to manipulate a storage.
#[async_trait]
pub trait Storage<O: Object, B: Bucket<O>> {
    /// Returns a bucket.
    async fn bucket(&self, name: &str) -> Result<B, O::Error>;

    /// Creates a bucket.
    async fn create_bucket(&self, name: &str) -> Result<B, O::Error>;

    /// Deletes a bucket.
    async fn delete_bucket(&self, name: &str) -> Result<(), O::Error>;
}
