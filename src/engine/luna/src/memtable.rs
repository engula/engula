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

use std::{collections::BTreeMap, ops::Bound::*, sync::Mutex};

use crate::{
    codec::{Timestamp, Value},
    scan::Scan,
    WriteBatch,
};

pub struct Memtable {
    inner: Mutex<Inner>,
}

type ValueTree = BTreeMap<Timestamp, Value>;
type Tree = BTreeMap<Vec<u8>, ValueTree>;

struct Inner {
    tree: Tree,
    last_ts: Timestamp,
    estimated_size: usize,
}

impl Memtable {
    pub fn new(ts: Timestamp) -> Self {
        let inner = Inner {
            tree: Tree::new(),
            last_ts: ts,
            estimated_size: 0,
        };
        Memtable {
            inner: Mutex::new(inner),
        }
    }

    pub fn write(&self, batch: WriteBatch) {
        let mut inner = self.inner.lock().unwrap();
        inner.last_ts = batch.ts;
        inner.estimated_size += batch.estimated_size;
        for w in batch.writes {
            let vtree = inner.tree.entry(w.0).or_insert_with(BTreeMap::new);
            vtree.insert(batch.ts, w.1);
        }
    }

    pub fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        inner.tree.get(key).and_then(|vtree| {
            vtree
                .range((Unbounded, Included(ts)))
                .next_back()
                .and_then(|x| x.1.clone())
        })
    }

    pub fn scan(&self) -> MemtableScanner {
        todo!();
    }

    pub fn last_timestamp(&self) -> Timestamp {
        self.inner.lock().unwrap().last_ts
    }

    pub fn estimated_size(&self) -> usize {
        self.inner.lock().unwrap().estimated_size
    }
}

pub struct MemtableScanner {}

impl Scan for MemtableScanner {
    fn seek_to_first(&mut self) {
        todo!();
    }

    fn seek(&mut self) {
        todo!();
    }

    fn next(&mut self) {
        todo!();
    }

    fn valid(&self) -> bool {
        todo!();
    }

    fn key(&self) -> &[u8] {
        todo!();
    }

    fn value(&self) -> &[u8] {
        todo!();
    }
}
