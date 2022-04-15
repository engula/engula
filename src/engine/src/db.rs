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

use crate::{key_space::KeySpace, objects::ObjectRef};

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
    pub fn get(&self, key: &[u8]) -> Option<ObjectRef> {
        let mut state = self.state.lock().unwrap();
        state.key_space.get(key).cloned()
    }

    pub fn insert(&self, object_ref: ObjectRef) {
        let mut state = self.state.lock().unwrap();

        let key = object_ref.key();
        if let Some(old_object_ref) = state.key_space.insert(key, object_ref) {
            old_object_ref.drop_in_place();
        }
    }

    pub fn remove(&self, key: &[u8]) -> Option<ObjectRef> {
        let mut state = self.state.lock().unwrap();
        state.key_space.remove(key)
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
