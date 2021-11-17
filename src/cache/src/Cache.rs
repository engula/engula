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

use std::{borrow::Borrow, hash::Hash};

pub trait Cache {
    type Key: Hash + Eq;
    type Value;

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Option<Self::Value>;

    fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Self::Value>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq;

    fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<Self::Value>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq;

    fn clear(&mut self);
}
