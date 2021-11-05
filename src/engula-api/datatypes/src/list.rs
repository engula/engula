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

use std::marker::PhantomData;

use crate::DataValue;

#[derive(Default)]
pub struct List<V>(V);

impl<V: DataValue> DataValue for List<V> {
    type Mutation = ListMutation<V>;
    type Value = ListValue<V::Value>;
}

pub type ListValue<V> = Vec<V>;

#[derive(Default)]
pub struct ListMutation<V> {
    _v: PhantomData<V>,
}

impl<V: DataValue> ListMutation<V> {
    pub fn append(&mut self, _v: V::Value) -> &mut Self {
        self
    }

    pub fn update(&mut self, _i: u64, _m: V::Mutation) -> &mut Self {
        self
    }
}
