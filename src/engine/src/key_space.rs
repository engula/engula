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
use std::hash::{Hash, Hasher};

use hashbrown::raw::RawTable;

use crate::objects::ObjectRef;

const MIN_NUM_BUCKETS: usize = 16;
const ADVANCE_STEP: usize = 64;

#[repr(C)]
struct ObjectEntry {
    hash: u64,
    object_ref: ObjectRef,
}

pub struct KeySpace {
    current_space: RawTable<ObjectEntry>,
    next_space: Option<RawTable<ObjectEntry>>,
}

impl KeySpace {
    pub fn new() -> KeySpace {
        KeySpace {
            current_space: RawTable::with_capacity(MIN_NUM_BUCKETS),
            next_space: None,
        }
    }

    pub fn insert(&mut self, key: &[u8], val: ObjectRef) -> Option<ObjectRef> {
        self.advance_rehash();

        let hash = make_hash(&key);
        let entry = ObjectEntry {
            object_ref: val,
            hash,
        };

        if let Some(next_table) = self.next_space.as_mut() {
            if let Some(record) = next_table.get_mut(hash, equivalent_key(key)) {
                Some(std::mem::replace(record, entry).object_ref)
            } else {
                unsafe { next_table.insert_no_grow(hash, entry) };
                self.current_space.remove_entry(hash, equivalent_key(key));
                None
            }
        } else if let Some(record) = self.current_space.get_mut(hash, equivalent_key(key)) {
            Some(std::mem::replace(record, entry).object_ref)
        } else {
            unsafe { self.current_space.insert_no_grow(hash, entry) };
            self.ensure_enough_space();
            None
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<ObjectRef> {
        self.advance_rehash();

        let hash = make_hash(&key);
        if let Some(next_table) = self.next_space.as_mut() {
            if let Some(record) = next_table.remove_entry(hash, equivalent_key(key)) {
                return Some(record.object_ref);
            }
        }

        if let Some(record) = self.current_space.remove_entry(hash, equivalent_key(key)) {
            Some(record.object_ref)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut ObjectRef> {
        self.advance_rehash();

        let hash = make_hash(&key);
        if let Some(next_table) = self.next_space.as_mut() {
            if let Some(entry) = next_table.get_mut(hash, equivalent_key(key)) {
                return Some(&mut entry.object_ref);
            }
        }
        match self.current_space.get_mut(hash, equivalent_key(key)) {
            Some(entry) => Some(&mut entry.object_ref),
            None => None,
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&ObjectRef> {
        self.advance_rehash();

        let hash = make_hash(&key);
        if let Some(next_table) = self.next_space.as_mut() {
            if let Some(entry) = next_table.get_mut(hash, equivalent_key(key)) {
                return Some(&entry.object_ref);
            }
        }
        match self.current_space.get(hash, equivalent_key(key)) {
            Some(entry) => Some(&entry.object_ref),
            None => None,
        }
    }

    pub fn advance_rehash(&mut self) {
        if let Some(next_table) = self.next_space.as_mut() {
            unsafe {
                let mut advanced: usize = 0;
                for bucket in self.current_space.iter() {
                    // SAFETY:
                    // 1. bucket read from current space
                    // 2. there no any conflicts
                    let entry = self.current_space.remove(bucket);
                    next_table.insert_no_grow(entry.hash, entry);
                    advanced += 1;
                    if advanced > ADVANCE_STEP {
                        return;
                    }
                }

                // Rehash is finished.
                std::mem::swap(next_table, &mut self.current_space);
                self.next_space = None;
            }
        }
    }

    fn ensure_enough_space(&mut self) {
        if self.next_space.is_some() {
            return;
        }

        // Only consider expansion.
        let cap = self.current_space.capacity();
        let len = self.current_space.len();
        if len * 100 >= 86 * cap {
            self.next_space = Some(RawTable::with_capacity(cap * 2));
            self.advance_rehash();
        }
    }
}

impl Default for KeySpace {
    fn default() -> Self {
        KeySpace::new()
    }
}

fn make_hash<K>(val: &K) -> u64
where
    K: Hash,
{
    use std::collections::hash_map::DefaultHasher;
    let mut state = DefaultHasher::new();
    val.hash(&mut state);
    state.finish()
}

fn equivalent_key(k: &[u8]) -> impl Fn(&ObjectEntry) -> bool + '_ {
    move |x| k.eq(x.object_ref.key())
}
