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
    indices: HashMap<u64, u32>,
    entries: Vec<HashEntry>,
    // The position of the next free entry.
    next: u32,
}

impl HashTable {
    pub fn with_mem_capacity(mem_capacity: usize) -> HashTable {
        let capacity = mem_capacity / (12 + std::mem::size_of::<HashEntry>());
        let mut table = Self {
            indices: HashMap::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            next: 1,
        };
        // Inserts a dummy entry.
        table.entries.push(HashEntry::default());
        table
    }

    pub fn get(&self, key: &[u8]) -> Option<&HashEntry> {
        let hash = hash64(key);
        if let Some(index) = self.indices.get(&hash) {
            self.entries.get(*index as usize)
        } else {
            None
        }
    }

    pub fn next(&self, index: u32) -> Option<&HashEntry> {
        if index > 0 {
            self.entries.get(index as usize)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: &[u8], handle: BlockHandle) {
        let hash = hash64(key);
        let next = self.indices.get(&hash).cloned().unwrap_or_default();
        let entry = HashEntry {
            fileno: handle.fileno,
            offset: handle.offset,
            length: handle.length,
            next,
        };
        let index = self.next;
        if index == self.entries.len() as u32 {
            self.entries.push(entry);
            self.next = index + 1;
        } else {
            let entry = std::mem::replace(&mut self.entries[index as usize], entry);
            self.next = entry.next;
        }
        self.indices.insert(hash, index);
    }
}

#[derive(Clone, Default)]
pub struct HashEntry {
    pub fileno: u32,
    pub offset: u32,
    pub length: u32,
    pub next: u32,
}

impl HashEntry {
    pub fn as_handle(&self) -> BlockHandle {
        BlockHandle {
            fileno: self.fileno,
            offset: self.offset,
            length: self.length,
        }
    }
}

fn hash64(buf: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::default();
    hasher.write(buf);
    hasher.finish()
}
