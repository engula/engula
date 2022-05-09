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

use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::trace;

use crate::{
    diskcache::DiskCache,
    elements::{array::Array, BoxElement},
    key_space::KeySpace,
    objects::{BoxObject, ObjectLayout, RawObject, RawString},
    stats::DbStats,
};

#[derive(Clone)]
pub struct Db {
    state: Arc<Mutex<DbState>>,
}

unsafe impl Sync for Db {}
unsafe impl Send for Db {}

struct DbState {
    lru_clock: u32,
    num_tick: usize,
    /// The size of memory used after system initialization.
    // initial_used: usize,
    max_memory: usize,
    num_evicted: usize,
    key_space: KeySpace,
    disk_cache: DiskCache,
}

impl Db {
    pub fn new(max_memory: usize, disk_cache: DiskCache) -> Self {
        Db {
            state: Arc::new(Mutex::new(DbState {
                lru_clock: 0,
                num_tick: 0,
                max_memory,
                num_evicted: 0,
                // initial_used: crate::alloc::allocated(),
                key_space: KeySpace::default(),
                disk_cache,
            })),
        }
    }

    pub async fn get(&self, key: &[u8]) -> Option<RawObject> {
        let mut state = self.state.lock().await;
        match state.key_space.get(key) {
            Some(mut raw_object) => {
                let object_meta = raw_object.object_meta_mut();
                if object_meta.lru() != state.lru_clock {
                    object_meta.set_lru(state.lru_clock);
                }
                if object_meta.is_evicted() {
                    trace!(key = ?key, "load key from disk cache");
                    if let Some(value) = state.disk_cache.get(key).await.unwrap() {
                        let string = raw_object
                            .data_mut::<RawString>()
                            .expect("Only support RawString");
                        let mut element = BoxElement::<Array>::with_capacity(value.len());
                        element.data_slice_mut().copy_from_slice(&value);
                        assert!(string.update_value(Some(element)).is_none());
                        string.meta.clear_evicted();
                    } else {
                        // This key has been evicted from disk cache.
                        state.key_space.remove(key);
                        return None;
                    }
                }
                Some(raw_object)
            }
            None => None,
        }
    }

    pub async fn insert<T>(&self, mut object: BoxObject<T>)
    where
        T: ObjectLayout,
    {
        let mut state = self.state.lock().await;

        debug_assert!(!object.meta.is_evicted());
        debug_assert!(!object.meta.is_tombstone());

        object.meta.set_lru(state.lru_clock);
        Self::ensure_memory_hard_limit(&mut *state).await;
        state.key_space.insert(object);
    }

    pub async fn remove(&self, key: &[u8]) {
        let mut state = self.state.lock().await;
        state.key_space.remove(key);
    }

    pub async fn delete_keys<'a, K: IntoIterator<Item = &'a [u8]>>(&self, keys: K) -> u64 {
        let mut state = self.state.lock().await;
        let mut num_deleted = 0;
        for key in keys {
            if state.key_space.remove(key) {
                num_deleted += 1;
            }
        }
        num_deleted
    }

    pub async fn on_tick(&self) {
        let mut state = self.state.lock().await;
        state.num_tick += 1;
        if state.num_tick % 10 == 0 {
            state.lru_clock = state.lru_clock.wrapping_add(1);
        }
        state.key_space.recycle_some_expired_keys();
        state.key_space.try_resize_space();
    }

    pub async fn stats(&self) -> DbStats {
        let state = self.state.lock().await;
        let mut stats = state.key_space.stats();
        stats.evicted_keys = state.num_evicted;
        stats
    }

    async fn ensure_memory_hard_limit(state: &mut DbState) {
        loop {
            let memory_used = crate::alloc::allocated();
            if memory_used < state.max_memory {
                return;
            }

            // try evict some keys to disk.
            let mut raw_object = match state.key_space.select_random_object() {
                Some(raw_object) => raw_object,
                None => break,
            };

            // TODO(walter) now only support `RawString`.
            if let Some(string) = raw_object.data_mut::<RawString>() {
                trace!(key = ?string.key(), "evict key to disk cache");
                state.num_evicted += 1;
                state
                    .disk_cache
                    .set(string.key(), string.data_slice())
                    .await
                    .unwrap();
                string.meta.set_evicted();
                string.update_value(None);
            }
        }
    }
}
