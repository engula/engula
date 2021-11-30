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

extern crate linked_hash_map;

use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash},
    option::Option,
};

use async_trait::async_trait;
use linked_hash_map::LinkedHashMap;
use tokio::sync::Mutex;

use crate::{Cache, Count, Countable, Measure_trait};

#[derive(Clone)]
pub struct LruCache<
    K: Hash + Eq + Sync + Send,
    V: Send + Clone,
    S: BuildHasher = DefaultHashBuilder,
    M: Countable<K, V> = Count,
> {
    queue: Mutex<LinkedHashMap<K, V, S>>,
    max_size: usize,
    current_measure: M::Measure,
    measure: M,
}

#[async_trait]
impl<K: Hash + Eq + Sync + Send, V: Send + Clone, S: BuildHasher, M: Measure_trait<K, V>>
    Cache<K, V, S, M> for LruCache<K, V, S, M>
{
    fn with_meter_and_hasher(size: usize, m: M, s: S) -> Self {
        LruCache {
            queue: Mutex::new(LinkedHashMap::with_hasher(s)),
            current_measure: Default::default(),
            max_size: size,
            measure: m,
        }
    }

    async fn insert(&mut self, k: K, v: V) -> Option<V> {
        let new_size = self.measure.measure(&k, &v);
        self.current_measure = self.measure.add(self.current_measure, new_size);
        if let Some(old) = self.queue.lock().await.get(&k) {
            self.current_measure = self
                .measure
                .sub(self.current_measure, self.measure.measure(&k, old));
        }
        let old_val = self.queue.lock().await.insert(k, v);
        while self.max_size() > self.max_size {
            self.queue.lock().await.pop_front();
        }
        old_val
    }

    async fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Sync + Send,
    {
        let mut map = self.queue.lock().await;
        map.get_refresh(key).cloned()
    }

    async fn remove<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Sync + Send,
    {
        let mut map = self.queue.lock().await;
        map.remove(key)
    }

    async fn clear(&self) {
        let mut map = self.queue.lock().await;
        map.clear()
    }

    fn size(&self) -> usize {
        self.measure
            .size(self.current_measure)
            .unwrap_or_else(|| self.queue.lock().await.len())
    }
}

impl<K: Eq + Hash + Sync + Send, V: Send + Clone> LruCache<K, V> {
    #[allow(dead_code)]
    pub fn new(size: usize) -> LruCache<K, V> {
        LruCache {
            queue: Mutex::new(LinkedHashMap::new()),
            max_size: size,
            current_measure: (),
            measure: Count,
        }
    }
}

impl<K: Eq + Hash + Sync + Send, V: Send + Clone, M: Countable<K, V>> LruCache<K, V, M> {
    pub fn with_measure(size: usize, measure: M) -> LruCache<K, V, M> {
        LruCache {
            queue: Mutex::new(LinkedHashMap::new()),
            current_measure: Default::default(),
            max_size: size,
            measure,
        }
    }
}

impl<K: Eq + Hash + Sync + Send, V: Send + Clone, S: BuildHasher> LruCache<K, V, S, Count> {
    pub fn with_hasher(size: usize, hash_builder: S) -> LruCache<K, V, S, Count> {
        LruCache {
            queue: Mutex::new(LinkedHashMap::with_hasher(hash_builder)),
            current_measure: (),
            max_size: size,
            measure: Count,
        }
    }
}
