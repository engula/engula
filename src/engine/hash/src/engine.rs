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

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{memtable::Memtable, Result};

pub struct Engine {
    inner: Mutex<Inner>,
}

struct Inner {
    mem: Arc<Memtable>,
    imm: Vec<Arc<Memtable>>,
}

const MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

impl Engine {
    pub fn new() -> Self {
        let inner = Inner {
            mem: Arc::new(Memtable::new()),
            imm: Vec::new(),
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().await;
        if let Some(value) = inner.mem.get(&key).await {
            return Ok(Some(value));
        }
        for imm in &inner.imm {
            if let Some(value) = imm.get(&key).await {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub async fn set<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(&self, key: K, value: V) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.mem.set(key, value).await;
        if inner.mem.approximate_size() > MEMTABLE_SIZE {
            let mem = inner.mem.clone();
            inner.imm.push(mem);
            inner.mem = Arc::new(Memtable::new());
        }
        Ok(())
    }
}
