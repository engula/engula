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
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

#[derive(Clone)]
pub struct Db {
    table: Arc<Mutex<HashMap<Bytes, Bytes>>>,
}

impl Default for Db {
    fn default() -> Self {
        Self {
            table: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Db {
    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        let table = self.table.lock().unwrap();
        table.get(key).cloned()
    }

    pub fn set(&self, key: Bytes, value: Bytes) {
        let mut table = self.table.lock().unwrap();
        table.insert(key, value);
    }

    pub fn del(&self, keys: &[Bytes]) -> u64 {
        let mut table = self.table.lock().unwrap();
        let mut res = 0;
        for key in keys.iter() {
            if table.remove(key).is_some() {
                res += 1;
            }
        }
        res
    }
}
