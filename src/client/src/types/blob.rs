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

use std::ops::RangeBounds;

use engula_apis::v1::*;

use super::{Mutate, Select};

pub struct Blob;

impl Blob {
    pub fn value(value: impl Into<Vec<u8>>) -> Vec<u8> {
        value.into()
    }

    pub fn range(range: impl RangeBounds<i64>) -> Select {
        let index = RangeValue::from_bounds(range);
        Select::default().index(index)
    }

    pub fn len() -> Select {
        Select::default().len()
    }

    pub fn trim(range: impl RangeBounds<i64>) -> Mutate {
        let index = RangeValue::from_bounds(range);
        Mutate::default().trim(index)
    }

    pub fn lpop(count: i64) -> Mutate {
        Mutate::default().lpop(count)
    }

    pub fn rpop(count: i64) -> Mutate {
        Mutate::default().rpop(count)
    }

    pub fn lpush(value: impl Into<Vec<u8>>) -> Mutate {
        Mutate::default().lpush(value.into())
    }

    pub fn rpush(value: impl Into<Vec<u8>>) -> Mutate {
        Mutate::default().rpush(value.into())
    }
}
