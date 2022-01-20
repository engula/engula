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
    collections::{btree_map, BTreeMap},
    ops::Bound::*,
    sync::{Arc, Mutex},
};

use uuid::Uuid;

use crate::{
    codec::{InternalKey, ParsedInternalKey, Timestamp, ValueKind},
    scan::Scan,
    WriteBatch,
};

pub struct Memtable {
    id: String,
    inner: Arc<Mutex<Inner>>,
}

type Tree = BTreeMap<InternalKey, Vec<u8>>;
type TreeIter = btree_map::IntoIter<InternalKey, Vec<u8>>;

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
            id: Uuid::new_v4().to_string(),
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn write(&self, batch: WriteBatch) {
        let mut inner = self.inner.lock().unwrap();
        inner.last_ts = batch.ts + batch.writes.len() as u64;
        inner.estimated_size += batch.estimated_size;
        let mut next_ts = batch.ts;
        for w in batch.writes {
            if let Some(value) = w.1 {
                let pk = ParsedInternalKey {
                    user_key: &w.0,
                    timestamp: next_ts,
                    value_kind: ValueKind::Some,
                };
                inner.tree.insert(pk.into(), value);
            } else {
                let pk = ParsedInternalKey {
                    user_key: &w.0,
                    timestamp: next_ts,
                    value_kind: ValueKind::None,
                };
                inner.tree.insert(pk.into(), Vec::new());
            }
            next_ts += 1;
        }
    }

    pub fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        let lookup_key = InternalKey::for_lookup(key, ts);
        inner
            .tree
            .range((Included(lookup_key), Unbounded))
            .next()
            .and_then(|x| {
                let pk = x.0.parse();
                match pk.value_kind {
                    ValueKind::None => None,
                    ValueKind::Some => Some(x.1.clone()),
                    _ => panic!(),
                }
            })
    }

    pub fn scan(&self, ts: Timestamp) -> MemtableScanner {
        MemtableScanner::new(self.inner.clone(), ts)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn last_timestamp(&self) -> Timestamp {
        self.inner.lock().unwrap().last_ts
    }

    pub fn estimated_size(&self) -> usize {
        self.inner.lock().unwrap().estimated_size
    }
}

pub struct MemtableScanner {
    inner: Arc<Mutex<Inner>>,
    ts: Timestamp,
    iter: Option<TreeIter>,
    item: Option<(InternalKey, Vec<u8>)>,
}

impl MemtableScanner {
    fn new(inner: Arc<Mutex<Inner>>, ts: Timestamp) -> Self {
        Self {
            inner,
            ts,
            iter: None,
            item: None,
        }
    }
}

impl Scan for MemtableScanner {
    fn seek_to_first(&mut self) {
        {
            let inner = self.inner.lock().unwrap();
            self.iter = Some(inner.tree.clone().into_iter());
        }
        self.next();
    }

    fn seek(&mut self, target: &[u8]) {
        {
            let inner = self.inner.lock().unwrap();
            let internal_target = InternalKey::new(target.to_owned());
            let tree: Tree = inner
                .tree
                .range((Included(internal_target), Unbounded))
                .map(|x| (x.0.clone(), x.1.clone()))
                .collect();
            self.iter = Some(tree.into_iter());
        }
        self.next();
    }

    fn next(&mut self) {
        if let Some(iter) = self.iter.as_mut() {
            for item in iter.by_ref() {
                let pk = item.0.parse();
                if pk.timestamp <= self.ts {
                    self.item = Some(item);
                    return;
                }
            }
            self.iter = None;
        }
        self.item = None;
    }

    fn valid(&self) -> bool {
        self.item.is_some()
    }

    fn key(&self) -> &[u8] {
        self.item.as_ref().unwrap().0.as_bytes()
    }

    fn value(&self) -> &[u8] {
        &self.item.as_ref().unwrap().1
    }
}
