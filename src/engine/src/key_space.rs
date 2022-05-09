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

use hashbrown::raw::{RawIterRange, RawTable};
use rand::{thread_rng, Rng};

use crate::{
    objects::{BoxObject, ObjectLayout, RawObject},
    stats::DbStats,
    time::unix_timestamp_millis,
};

const MIN_NUM_BUCKETS: usize = 16;
const ADVANCE_STEP: usize = 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct ObjectEntry {
    hash: u64,
    raw_object: RawObject,
}

trait DictValue {
    fn hash(&self) -> u64;
}

struct Dict<T: DictValue> {
    min_num_buckets: usize,
    current_table: RawTable<T>,
    next_table: Option<RawTable<T>>,
}

pub struct KeySpace {
    stats: DbStats,
    objects: Dict<ObjectEntry>,
    expires: Dict<ObjectEntry>,
}

impl<T: DictValue> Dict<T> {
    pub fn new(min_num_buckets: usize) -> Self {
        Dict {
            min_num_buckets,
            current_table: RawTable::with_capacity(min_num_buckets),
            next_table: None,
        }
    }

    pub fn insert(&mut self, value: T, eq: &impl Fn(&T) -> bool) -> Option<T> {
        self.advance_rehash();

        let hash = value.hash();
        if let Some(next_table) = self.next_table.as_mut() {
            if let Some(entry) = next_table.get_mut(hash, eq) {
                Some(std::mem::replace(entry, value))
            } else {
                unsafe { next_table.insert_no_grow(hash, value) };
                self.current_table.remove_entry(hash, eq);
                None
            }
        } else if let Some(entry) = self.current_table.get_mut(hash, eq) {
            Some(std::mem::replace(entry, value))
        } else {
            unsafe { self.current_table.insert_no_grow(hash, value) };
            self.try_expand_table();
            None
        }
    }

    pub fn remove(&mut self, hash: u64, eq: &impl Fn(&T) -> bool) -> Option<T> {
        self.advance_rehash();

        if let Some(next_table) = self.next_table.as_mut() {
            if let Some(record) = next_table.remove_entry(hash, eq) {
                return Some(record);
            }
        }

        self.current_table.remove_entry(hash, eq)
    }

    pub fn get_mut(&mut self, hash: u64, eq: &impl Fn(&T) -> bool) -> Option<&mut T> {
        self.advance_rehash();

        if let Some(next_table) = self.next_table.as_mut() {
            if let Some(entry) = next_table.get_mut(hash, eq) {
                return Some(entry);
            }
        }

        self.current_table.get_mut(hash, eq)
    }

    pub fn advance_rehash(&mut self) {
        if let Some(next_table) = self.next_table.as_mut() {
            unsafe {
                let mut advanced: usize = 0;
                for bucket in self.current_table.iter() {
                    // SAFETY:
                    // 1. bucket read from current space
                    // 2. there no any conflicts
                    let entry = self.current_table.remove(bucket);
                    next_table.insert_no_grow(T::hash(&entry), entry);
                    advanced += 1;
                    if advanced > ADVANCE_STEP {
                        return;
                    }
                }

                // Rehash is finished.
                std::mem::swap(next_table, &mut self.current_table);
                self.next_table = None;
            }
        }
    }

    fn try_expand_table(&mut self) {
        if self.next_table.is_some() {
            return;
        }

        let cap = self.current_table.capacity();
        let len = self.current_table.len();
        if len * 100 >= cap * 90 {
            self.next_table = Some(RawTable::with_capacity(cap * 2));
            self.advance_rehash();
        }
    }

    pub fn try_resize_table(&mut self) {
        if self.next_table.is_none() {
            return;
        }

        let len = self.current_table.len();
        let cap = self.current_table.capacity();
        if len * 100 / cap < 10 {
            let len = std::cmp::max(self.min_num_buckets, len);
            self.next_table = Some(RawTable::with_capacity(len.next_power_of_two()));
            self.advance_rehash();
        }
    }

    fn drain_next_table(&mut self) -> Option<impl Iterator<Item = T>> {
        self.next_table
            .take()
            .map(|next_table| next_table.into_iter())
    }

    fn drain_current_table(&mut self) -> impl Iterator<Item = T> + '_ {
        self.current_table.drain()
    }
}

impl Dict<ObjectEntry> {
    /// Select maximum `limit` objects randomly from key space.
    #[allow(dead_code)]
    pub fn random_objects(&mut self, limit: usize) -> Vec<RawObject> {
        let mut objects = Self::random_objects_from_space(&self.current_table, limit);
        let left = limit - objects.len();
        if left > 0 {
            if let Some(next_space) = self.next_table.as_ref() {
                objects.append(&mut Self::random_objects_from_space(next_space, left));
            }
        }
        objects
    }

    fn random_objects_from_space(space: &RawTable<ObjectEntry>, limit: usize) -> Vec<RawObject> {
        unsafe {
            let take_n_objects = |it: RawIterRange<ObjectEntry>, n| {
                it.take(n).map(|bucket| bucket.as_ref().raw_object)
            };
            let index = thread_rng().gen::<usize>();
            let limit = std::cmp::min(space.len(), limit);
            let mut objects =
                take_n_objects(space.iter_with_hint(index), limit).collect::<Vec<_>>();
            if objects.len() < limit {
                objects.extend(take_n_objects(
                    space.iter_with_hint(0),
                    limit - objects.len(),
                ));
            }
            objects
        }
    }
}

impl DictValue for ObjectEntry {
    fn hash(&self) -> u64 {
        self.hash
    }
}

impl KeySpace {
    pub fn new() -> KeySpace {
        KeySpace {
            stats: DbStats::default(),
            objects: Dict::new(MIN_NUM_BUCKETS),
            expires: Dict::new(MIN_NUM_BUCKETS),
        }
    }

    pub fn insert<T>(&mut self, object: BoxObject<T>)
    where
        T: ObjectLayout,
    {
        let key = object.key();
        let hash = make_hash(&key);
        let raw_object = BoxObject::leak(object);
        let entry = ObjectEntry { raw_object, hash };
        if let Some(prev_entry) = self.objects.insert(entry, &equivalent_key(key)) {
            if raw_object.object_meta().has_deadline() {
                self.expires.insert(entry, &equivalent_key(key));
            } else if prev_entry.raw_object.object_meta().has_deadline() {
                self.expires.remove(entry.hash, &equivalent_key(key));
            }
            prev_entry.raw_object.drop_in_place();
        } else {
            self.stats.num_keys += 1;
            if raw_object.object_meta().has_deadline() {
                self.expires.insert(entry, &equivalent_key(key));
            }
        }
    }

    /// Remove and recycle memory space of the corresponding key, returns `false` if no such key
    /// exists.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        let hash = make_hash(&key);
        if let Some(entry) = self.objects.remove(hash, &equivalent_key(key)) {
            self.expires.remove(entry.hash, &equivalent_key(key));
            entry.raw_object.drop_in_place();
            self.stats.num_keys -= 1;
            true
        } else {
            false
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<RawObject> {
        let hash = make_hash(&key);
        if let Some(entry) = self.objects.get_mut(hash, &equivalent_key(key)) {
            let raw_object = entry.raw_object;
            let object_meta = raw_object.object_meta();
            if object_meta.has_deadline() && object_meta.deadline() < unix_timestamp_millis() {
                // Remove expired key
                self.expires.remove(hash, &equivalent_key(key));
                self.objects.remove(hash, &equivalent_key(key));
                raw_object.drop_in_place();
                self.stats.num_keys -= 1;
                self.stats.expired_keys += 1;
                self.stats.keyspace_misses += 1;
                return None;
            }

            self.stats.keyspace_hits += 1;
            return Some(raw_object);
        }

        self.stats.keyspace_misses += 1;
        None
    }

    pub fn try_resize_space(&mut self) {
        self.objects.try_resize_table();
        self.expires.try_resize_table();
    }

    pub fn recycle_some_expired_keys(&mut self) {
        let now = unix_timestamp_millis();
        let raw_objects = self.expires.random_objects(16);
        for raw_object in raw_objects {
            let object_meta = raw_object.object_meta();
            debug_assert!(object_meta.has_deadline());
            if object_meta.deadline() < now {
                self.remove(raw_object.key());
                raw_object.drop_in_place();
                self.stats.expired_keys += 1;
            }
        }
    }

    pub fn select_random_object(&mut self) -> Option<RawObject> {
        let mut raw_objects = self.objects.random_objects(10);
        raw_objects.sort_unstable_by(compare_raw_objects);
        raw_objects
            .into_iter()
            .find(|raw_object| !raw_object.object_meta().is_evicted())
    }

    #[inline]
    pub fn stats(&self) -> DbStats {
        self.stats.clone()
    }
}

impl Default for KeySpace {
    fn default() -> Self {
        KeySpace::new()
    }
}

impl Drop for KeySpace {
    fn drop(&mut self) {
        for entry in self.objects.drain_current_table() {
            entry.raw_object.drop_in_place();
        }
        if let Some(next_table) = self.objects.drain_next_table() {
            for entry in next_table {
                entry.raw_object.drop_in_place();
            }
        }
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
    move |x| k.eq(x.raw_object.key())
}

fn compare_raw_objects(lhs: &RawObject, rhs: &RawObject) -> std::cmp::Ordering {
    lhs.object_meta().lru().cmp(&rhs.object_meta().lru())
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use super::*;
    use crate::objects::{BoxObject, RawString};

    #[test]
    fn random_objects_from_space() {
        unsafe {
            #[derive(Debug)]
            struct TestCase {
                objects: Vec<u64>,
                take: usize,
                expect: usize,
            }

            let cases = vec![
                TestCase {
                    objects: vec![],
                    take: 0,
                    expect: 0,
                },
                TestCase {
                    objects: vec![1, 3, 5, 7, 9],
                    take: 4,
                    expect: 4,
                },
                TestCase {
                    objects: vec![1, 3, 5, 7, 9],
                    take: 5,
                    expect: 5,
                },
                TestCase {
                    objects: vec![1, 3, 5, 7, 9],
                    take: 6,
                    expect: 5,
                },
            ];

            for case in cases {
                println!("case {:?}", case);
                let mut table = RawTable::<ObjectEntry>::with_capacity(32);
                for object in case.objects {
                    table.insert_no_grow(
                        object,
                        ObjectEntry {
                            hash: object,
                            raw_object: RawObject::from_raw(NonNull::dangling()),
                        },
                    );
                }

                let objects = Dict::random_objects_from_space(&table, case.take);
                assert_eq!(objects.len(), case.expect);
            }
        }
    }

    #[test]
    fn random_objects() {
        let mut space = Dict::<ObjectEntry>::new(MIN_NUM_BUCKETS);
        for v in [1, 2, 3, 4, 5, 6, 7, 8] {
            let object = BoxObject::<RawString>::with_key(&[v]);
            let entry = ObjectEntry {
                raw_object: BoxObject::leak(object),
                hash: make_hash(&[v]),
            };
            space.insert(entry, &equivalent_key(&[v]));
        }
        let objects = space.random_objects(0);
        assert_eq!(objects.len(), 0);

        let objects = space.random_objects(1);
        assert_eq!(objects.len(), 1);

        let objects = space.random_objects(10);
        assert_eq!(objects.len(), 8);
    }
}
