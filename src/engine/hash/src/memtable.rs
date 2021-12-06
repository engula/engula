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

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use tokio::sync::Mutex;

pub struct Memtable {
    map: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    size: AtomicUsize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(BTreeMap::new()),
            size: AtomicUsize::new(0),
        }
    }

    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Vec<u8>> {
        let map = self.map.lock().await;
        map.get(key.as_ref()).cloned()
    }

    pub async fn set<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(&self, key: K, value: V) {
        let key = key.into();
        let value = value.into();
        let entry_size = key.len() + value.len();
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        let mut map = self.map.lock().await;
        map.insert(key, value);
    }

    pub fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}
