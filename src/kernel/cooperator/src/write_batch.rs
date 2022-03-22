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

use engula_apis::v1::*;

pub enum Write {
    Put(Vec<u8>, Value),
    Delete(Vec<u8>),
}

#[derive(Default)]
pub struct WriteBatch {
    pub writes: Vec<Write>,
}

impl WriteBatch {
    pub fn put(&mut self, id: Vec<u8>, value: Value) {
        self.writes.push(Write::Put(id, value))
    }

    pub fn delete(&mut self, id: Vec<u8>) {
        self.writes.push(Write::Delete(id))
    }
}
