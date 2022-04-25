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

#[derive(Default)]
struct DbState {
    key_space: KeySpace,
}

impl Db {
    pub fn get(&self, key: &[u8]) -> Option<RawObject> {
        let mut state = self.state.lock().unwrap();
        state.key_space.get(key)
    }

    pub fn insert<T>(&self, object: BoxObject<T>)
    where
        T: ObjectLayout,
    {
        let mut state = self.state.lock().unwrap();

        let key = object.key();
        if let Some(raw_object) = state.key_space.insert(key, BoxObject::leak(object)) {
            raw_object.drop_in_place();
        }
    }

    pub fn remove(&self, key: &[u8]) {
        let mut state = self.state.lock().unwrap();
        if let Some(raw_object) = state.key_space.remove(key) {
            raw_object.drop_in_place();
        }
    }

    pub fn delete_keys<'a, K: IntoIterator<Item = &'a [u8]>>(&self, keys: K) -> u64 {
        let objects = {
            let mut state = self.state.lock().unwrap();
            keys.into_iter()
                .filter_map(|key| state.key_space.remove(key))
                .collect::<Vec<_>>()
        };

        let num_deleted = objects.len();
        for object_ref in objects {
            object_ref.drop_in_place();
        }
        num_deleted as u64
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
