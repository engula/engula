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
    stats::DbStats,
};

#[derive(Default, Clone)]
pub struct Db {
    state: Arc<Mutex<DbState>>,
}

unsafe impl Send for Db {}

unsafe impl Sync for Db {}

#[derive(Default)]
struct DbState {
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
                Some(raw_object)
            }
            None => None,
        }
    }

    pub fn insert<T>(&self, mut object: BoxObject<T>)
    where
        T: ObjectLayout,
    {
        let mut state = self.state.lock().unwrap();

        object.meta.set_lru(state.lru);
        state.key_space.insert(object);
    }

    pub fn remove(&self, key: &[u8]) {
        let mut state = self.state.lock().unwrap();
        state.key_space.remove(key);
    }

    pub fn delete_keys<'a, K: IntoIterator<Item = &'a [u8]>>(&self, keys: K) -> u64 {
        let mut state = self.state.lock().unwrap();
        let mut num_deleted = 0;
        for key in keys {
            if state.key_space.remove(key) {
                num_deleted += 1;
            }
        }
        num_deleted
    }

    pub fn on_tick(&self) {
        let mut state = self.state.lock().unwrap();
        state.key_space.recycle_some_expired_keys();
        state.key_space.try_resize_space();
    }

    pub fn stats(&self) -> DbStats {
        let state = self.state.lock().unwrap();
        state.key_space.stats()
    }
}
