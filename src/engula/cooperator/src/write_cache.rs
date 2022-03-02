// Copyright 2022 The Engula Authors.
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

use std::{collections::VecDeque, mem::size_of};

use tokio::sync::Mutex;

#[allow(dead_code)]
pub struct WriteCacheOptions {
    memtable_size: usize,
}

#[allow(dead_code)]
pub struct WriteCache {
    inner: Mutex<WriteCacheInner>,
}

struct WriteCacheInner {
    options: WriteCacheOptions,
    mem: Memtable,
    imm_list: VecDeque<Memtable>,
}

#[allow(dead_code)]
impl WriteCache {
    pub fn new(options: WriteCacheOptions) -> Self {
        let inner = WriteCacheInner {
            options,
            mem: Memtable::default(),
            imm_list: VecDeque::new(),
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub async fn add(&self, batch: WriteBatch) {
        let mut inner = self.inner.lock().await;
        inner.mem.add(batch);
        if inner.mem.approximate_size() >= inner.options.memtable_size {
            let imm = std::mem::take(&mut inner.mem);
            inner.imm_list.push_back(imm);
            // TODO: sorts and flushes immutable memtables.
        }
    }
}

#[derive(Default)]
struct Memtable {
    entries: Vec<Entry>,
    approximate_size: usize,
}

impl Memtable {
    fn add(&mut self, batch: WriteBatch) {
        self.approximate_size += batch.encoded_size();
        self.entries.extend(batch.entries);
    }

    fn approximate_size(&self) -> usize {
        self.approximate_size
    }
}

type Timestamp = u64;

enum Entry {
    Put(Vec<u8>, Timestamp, Vec<u8>),
    Delete(Vec<u8>, Timestamp),
}

impl Entry {
    fn encoded_size(&self) -> usize {
        let len = match self {
            Self::Put(id, _, value) => id.len() + value.len(),
            Self::Delete(id, _) => id.len(),
        };
        len + size_of::<Timestamp>()
    }
}

pub struct WriteBatch {
    entries: Vec<Entry>,
}

#[allow(dead_code)]
impl WriteBatch {
    pub fn put(&mut self, id: Vec<u8>, ts: Timestamp, value: Vec<u8>) {
        self.entries.push(Entry::Put(id, ts, value))
    }

    pub fn delete(&mut self, id: Vec<u8>, ts: Timestamp) {
        self.entries.push(Entry::Delete(id, ts))
    }

    pub fn encoded_size(&self) -> usize {
        self.entries.iter().map(|x| x.encoded_size()).sum()
    }
}
