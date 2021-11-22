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

use std::{borrow::Borrow, hash::Hash, option::Option};

use async_trait::async_trait;
use linked_hash_map::LinkedHashMap;
use tokio::sync::Mutex;

use crate::cache::Cache;

pub struct LruCache<K: Hash + Eq + Sync + Send, V: Send + Clone> {
    queue: Mutex<LinkedHashMap<K, V>>,
    max_size: usize,
}

#[async_trait]
impl<K: Hash + Eq + Sync + Send, V: Send + Clone> Cache for LruCache<K, V> {
    type Key = K;
    type Value = V;

    async fn insert(&self, key: Self::Key, value: Self::Value) -> (bool, Option<Self::Value>) {
        let mut map = self.queue.lock().await;
        map.insert(key, value);
        if map.len() > self.max_size {
            let (_, v) = map.pop_front().unwrap();
            return (true, Some(v));
        }
        return (true, None);
    }

    async fn get<Q: ?Sized>(&self, key: &Q) -> Option<Self::Value>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + Sync + Send,
    {
        let map = self.queue.lock().await;
        map.get(key).cloned()
    }

    async fn remove<Q: ?Sized>(&self, key: &Q) -> Option<Self::Value>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + Sync + Send,
    {
        let mut map = self.queue.lock().await;
        map.remove(key)
    }

    async fn clear(&self) {
        let mut map = self.queue.lock().await;
        map.clear()
    }
}

impl<K: Eq + Hash + Sync + Send, V: Send + Clone> LruCache<K, V> {
    #[allow(dead_code)]
    pub fn new(size: usize) -> Self {
        LruCache {
            queue: Mutex::new(LinkedHashMap::new()),
            max_size: size,
        }
    }
}
