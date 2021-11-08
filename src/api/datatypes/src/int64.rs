// Copyright 2021 The Engula Authors.
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

use crate::DataValue;

#[derive(Default)]
pub struct Int64();

impl DataValue for Int64 {
    type Mutation = Int64Mutation;
    type Value = Int64Value;
}

pub type Int64Value = i64;

#[derive(Default)]
pub struct Int64Mutation {}

impl Int64Mutation {
    pub fn add(&mut self, _v: i64) -> &mut Int64Mutation {
        self
    }

    pub fn sub(&mut self, _v: i64) -> &mut Int64Mutation {
        self
    }
}
