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

pub struct HashMap<K, V>(K, V);

impl<K, V: DataValue> DataValue for HashMap<K, V> {
    type Mutation = HashMapMutation<K, V>;
    type Value = HashMapValue<K, V>;
}

pub type HashMapValue<K, V> = std::collections::HashMap<K, V>;

#[derive(Default)]
pub struct HashMapMutation<K, V> {
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V: DataValue> HashMapMutation<K, V> {
    pub fn insert(&mut self, _k: K, _v: V::Value) -> &mut Self {
        self
    }

    pub fn update(&mut self, _k: K, _v: V::Mutation) -> &mut Self {
        self
    }
}
