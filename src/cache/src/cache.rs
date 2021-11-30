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
pub mod lru_cache;
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash},
};

use async_trait::async_trait;

use crate::Measure_trait;

#[async_trait]
pub trait Cache<K, V, S, M>
where
    K: Hash + Eq + Sync + Send,
    V: Send + Clone,
    M: Measure_trait<K, V>,
    S: BuildHasher,
{
    /// Creates an empty cache that can hold at most `size` as measured by
    /// `measure_trait` with the given hash builder.
    fn with_meter_and_hasher(size: usize, m: M, s: S) -> Self;
    // Inserts an item into the cache. If the key already existed, the old value is
    // returned.
    async fn insert(&self, k: K, v: V) -> Option<V>;
    //  Returns a reference to the value corresponding to the given key in the cache
    async fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Sync + Send;

    // Remove an item in the cache
    async fn remove<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Sync + Send;

    // Clear the cache
    async fn clear(&self);
    fn size(&self) -> usize;
}
