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

use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::Hasher,
};

use super::BlockHandle;

pub struct HashTable {
    map: HashMap<u64, u32>,
    slab: Vec<HashIndex>,
    // Points to the next free index.
    next: u32,
}

impl HashTable {
    pub fn with_mem_capacity(mem_capacity: usize) -> HashTable {
        let capacity = mem_capacity / (16 + std::mem::size_of::<HashIndex>());
        let mut table = Self {
            map: HashMap::with_capacity(capacity),
            slab: Vec::with_capacity(capacity),
            next: 1,
        };
        // Marks the 0th index as invalid.
        table.slab.push(HashIndex::default());
        table
    }

    pub fn get(&self, key: &[u8]) -> Option<&HashIndex> {
        let hash = hash64(key);
        if let Some(pos) = self.map.get(&hash) {
            self.slab.get(*pos as usize)
        } else {
            None
        }
    }

    pub fn next(&self, pos: u32) -> Option<&HashIndex> {
        if pos > 0 {
            self.slab.get(pos as usize)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: &[u8], handle: BlockHandle) {
        let hash = hash64(key);
        let next = self.map.get(&hash).cloned().unwrap_or_default();
        let index = HashIndex {
            fileno: handle.fileno,
            offset: handle.offset,
            length: handle.length,
            next,
        };
        // TODO: limits memory usage
        let pos = self.next;
        if pos == self.slab.len() as u32 {
            self.slab.push(index);
            self.next = pos + 1;
        } else {
            let index = std::mem::replace(&mut self.slab[pos as usize], index);
            self.next = index.next;
        }
        self.map.insert(hash, pos);
    }
}

#[derive(Clone, Default)]
pub struct HashIndex {
    pub fileno: u32,
    pub offset: u32,
    pub length: u32,
    pub next: u32,
}

impl HashIndex {
    pub fn as_handle(&self) -> BlockHandle {
        BlockHandle {
            fileno: self.fileno,
            offset: self.offset,
            length: self.length,
        }
    }
}

fn hash64(buf: &[u8]) -> u64 {
    let mut h = DefaultHasher::default();
    h.write(buf);
    h.finish()
}
