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

use std::collections::BTreeMap;

use tokio::sync::Mutex;

use crate::codec::{self, Timestamp};

pub struct Memtable {
    inner: Mutex<Inner>,
}

struct Inner {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
    size: usize,
    last_ts: Timestamp,
}

impl Memtable {
    pub fn new(ts: Timestamp) -> Self {
        let inner = Inner {
            map: BTreeMap::new(),
            size: 0,
            last_ts: ts,
        };
        Memtable {
            inner: Mutex::new(inner),
        }
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.lock().await;
        inner.map.get(key).cloned()
    }

    pub async fn set(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) {
        let mut inner = self.inner.lock().await;
        inner.size += codec::record_size(&key, &value);
        assert!(ts > inner.last_ts);
        inner.last_ts = ts;
        inner.map.insert(key, value);
    }

    pub async fn iter(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let inner = self.inner.lock().await;
        inner.map.clone()
    }

    pub async fn approximate_size(&self) -> usize {
        self.inner.lock().await.size
    }

    pub async fn last_update_timestamp(&self) -> Timestamp {
        self.inner.lock().await.last_ts
    }
}
