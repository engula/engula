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

use std::sync::{Arc, Mutex};

use crate::{
    key_space::KeySpace,
    objects::{BoxObject, ObjectLayout, RawObject},
};

#[derive(Default, Clone)]
pub struct Db {
    state: Arc<Mutex<DbState>>,
}

unsafe impl Send for Db {}

unsafe impl Sync for Db {}

#[derive(Default, Clone, Debug)]
pub struct DbStats {
    pub num_keys: usize,
    pub evicted_keys: usize,
    pub keyspace_hits: usize,
    pub keyspace_misses: usize,
}

#[derive(Default)]
struct DbState {
    stats: DbStats,
    lru: u32,
    key_space: KeySpace,
}

impl Db {
    pub fn get(&self, key: &[u8]) -> Option<RawObject> {
        let mut state = self.state.lock().unwrap();
        match state.key_space.get(key) {
            Some(mut raw_object) => {
                unsafe {
                    let object_meta = raw_object.object_meta_mut();
                    if object_meta.lru() != state.lru {
                        object_meta.set_lru(state.lru);
                    }
                };
                state.stats.keyspace_hits += 1;
                Some(raw_object)
            }
            None => {
                state.stats.keyspace_misses += 1;
                None
            }
        }
    }

    pub fn insert<T>(&self, mut object: BoxObject<T>)
    where
        T: ObjectLayout,
    {
        self.keep_memory_watermark();

        let mut state = self.state.lock().unwrap();
        let key = object.key();
        object.meta.set_lru(state.lru);
        if let Some(raw_object) = state.key_space.insert(key, BoxObject::leak(object)) {
            raw_object.drop_in_place();
        } else {
            state.stats.num_keys += 1;
        }
    }

    pub fn remove(&self, key: &[u8]) {
        let mut state = self.state.lock().unwrap();
        if let Some(raw_object) = state.key_space.remove(key) {
            raw_object.drop_in_place();
            state.stats.num_keys = state.stats.num_keys.saturating_sub(1);
        }
    }

    pub fn delete_keys<'a, K: IntoIterator<Item = &'a [u8]>>(&self, keys: K) -> u64 {
        let objects = {
            let mut state = self.state.lock().unwrap();
            let objects = keys
                .into_iter()
                .filter_map(|key| state.key_space.remove(key))
                .collect::<Vec<_>>();
            state.stats.num_keys = state.stats.num_keys.saturating_sub(objects.len());
            objects
        };

        let num_deleted = objects.len();
        for object_ref in objects {
            object_ref.drop_in_place();
        }
        num_deleted as u64
    }

    pub fn stats(&self) -> DbStats {
        let state = self.state.lock().unwrap();
        state.stats.clone()
    }

    pub fn on_cron(&self) {
        let mut state = self.state.lock().unwrap();
        state.lru = state.lru.wrapping_add(1);
    }

    pub fn before_sleep(&self) {
        let mut state = self.state.lock().unwrap();
        todo!()
    }

    fn keep_memory_watermark(&self) {
        let allocated_memory = crate::alloc::allocated_memory();
        let max_memory: usize = 100 * 1024 * 1024;
        if allocated_memory < max_memory {
            return;
        }

        println!(
            "allocated memory {} exceeds max memory {}",
            allocated_memory, max_memory
        );

        loop {
            unsafe {
                crate::alloc::compact_segment(|record_base| {
                    crate::compact::migrate_record(self.clone(), record_base)
                });
            }

            if crate::alloc::allocated_memory() >= max_memory {
                self.evict_one_key();
            } else {
                break;
            }
        }
    }

    fn evict_one_key(&self) {
        let mut state = self.state.lock().unwrap();
        let mut pool: [Option<RawObject>; 10] = [None; 10];
        let num_objects = state.key_space.random_objects(&mut pool[..]);
        pool[..num_objects]
            .sort_unstable_by(|a, b| a.map(RawObject::lru).cmp(&b.map(RawObject::lru)));
        let raw_object = pool
            .into_iter()
            .find(|v| v.is_some())
            .flatten()
            .expect("evict_one_key");

        state
            .key_space
            .remove(raw_object.key())
            .expect("evict_one_key");
        raw_object.drop_in_place();
        state.stats.num_keys = state.stats.num_keys.saturating_sub(1);
    }
}

impl Drop for DbState {
    fn drop(&mut self) {
        if let Some(drain) = self.key_space.drain_next_space() {
            for raw_object in drain {
                raw_object.drop_in_place();
            }
        }
        for raw_object in self.key_space.drain_current_space() {
            raw_object.drop_in_place();
        }
    }
}
