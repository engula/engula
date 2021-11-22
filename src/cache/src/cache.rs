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

use std::{borrow::Borrow, hash::Hash};

use async_trait::async_trait;

#[async_trait]
pub trait Cache {
    type Key: Hash + Eq + Sync + Send;
    type Value: Send + Clone;

    /// Insert an item in the cache.
    ///
    /// The first return value indicates whether an insertion has taken place
    /// (because the cache can refuse to insert an item). The second return
    /// value is the optional eviction victim, returned only if this call to
    /// insert caused an eviction.

    async fn insert(&self, key: Self::Key, value: Self::Value) -> Option<Self::Value>;

    async fn get<Q: ?Sized>(&self, key: &Q) -> Option<Self::Value>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + Sync + Send;

    async fn remove<Q: ?Sized>(&self, key: &Q) -> Option<Self::Value>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + Sync + Send;

    async fn clear(&self);
}
