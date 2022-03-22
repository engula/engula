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

use super::{Mutate, Select};

pub struct Map;

impl Map {
    pub fn value(value: impl Into<MapValue>) -> MapValue {
        value.into()
    }

    pub fn index(index: impl Into<Value>) -> Select {
        Select::default().index(index)
    }

    pub fn len() -> Select {
        Select::default().len()
    }

    pub fn clear() -> Mutate {
        Mutate::default().clear()
    }

    pub fn extend(value: impl Into<MapValue>) -> Mutate {
        Mutate::default().extend(value.into())
    }

    pub fn remove(index: impl Into<Value>) -> Mutate {
        Mutate::default().remove(index)
    }
}
