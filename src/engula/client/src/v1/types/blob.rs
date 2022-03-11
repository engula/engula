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

use super::{call, MutateExpr, SelectExpr};

pub struct Blob {}

impl Blob {
    pub fn len() -> BlobSelect {
        BlobSelect::len()
    }

    pub fn set(value: Vec<u8>) -> BlobMutate {
        BlobMutate::set(value)
    }

    pub fn pop_back(count: i64) -> BlobMutate {
        BlobMutate::pop_back(count)
    }

    pub fn pop_front(count: i64) -> BlobMutate {
        BlobMutate::pop_front(count)
    }

    pub fn push_back(value: Vec<u8>) -> BlobMutate {
        BlobMutate::push_back(value)
    }

    pub fn push_front(value: Vec<u8>) -> BlobMutate {
        BlobMutate::push_front(value)
    }
}

pub struct BlobSelect {
    expr: BlobExpr,
}

impl BlobSelect {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: BlobExpr { call: Some(call) },
        }
    }

    pub fn len() -> Self {
        Self::new(call::len())
    }
}

impl From<BlobSelect> for SelectExpr {
    fn from(v: BlobSelect) -> Self {
        TypedExpr::from(v.expr).into()
    }
}

pub struct BlobMutate {
    expr: BlobExpr,
}

impl BlobMutate {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: BlobExpr { call: Some(call) },
        }
    }

    pub fn set(value: Vec<u8>) -> Self {
        Self::new(call::set(value))
    }

    pub fn pop_back(count: i64) -> Self {
        Self::new(call::pop_back(count))
    }

    pub fn pop_front(count: i64) -> Self {
        Self::new(call::pop_front(count))
    }

    pub fn push_back(value: Vec<u8>) -> Self {
        Self::new(call::push_back(value))
    }

    pub fn push_front(value: Vec<u8>) -> Self {
        Self::new(call::push_front(value))
    }
}

impl From<BlobMutate> for MutateExpr {
    fn from(v: BlobMutate) -> Self {
        TypedExpr::from(v.expr).into()
    }
}
