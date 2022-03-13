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

use super::{call, MutateExpr, SelectExpr};

pub struct Blob(Vec<u8>);

impl From<Blob> for Value {
    fn from(v: Blob) -> Self {
        v.0.into()
    }
}

impl Blob {
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    pub fn len() -> BlobSelect {
        BlobSelect::len()
    }

    pub fn range(range: impl RangeBounds<i64>) -> BlobSelect {
        BlobSelect::range(range)
    }

    pub fn trim(range: impl RangeBounds<i64>) -> BlobMutate {
        BlobMutate::trim(range)
    }

    pub fn lpop(count: i64) -> BlobMutate {
        BlobMutate::lpop(count)
    }

    pub fn rpop(count: i64) -> BlobMutate {
        BlobMutate::rpop(count)
    }

    pub fn lpush(value: impl Into<Vec<u8>>) -> BlobMutate {
        BlobMutate::lpush(value)
    }

    pub fn rpush(value: impl Into<Vec<u8>>) -> BlobMutate {
        BlobMutate::rpush(value)
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

    pub fn range(range: impl RangeBounds<i64>) -> Self {
        Self::new(call::get_range(call::range(range)))
    }
}

impl From<BlobSelect> for SelectExpr {
    fn from(v: BlobSelect) -> Self {
        Expr::from(v.expr).into()
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

    pub fn trim(range: impl RangeBounds<i64>) -> Self {
        Self::new(call::trim(call::range(range)))
    }

    pub fn lpop(count: i64) -> Self {
        Self::new(call::lpop(count))
    }

    pub fn rpop(count: i64) -> Self {
        Self::new(call::rpop(count))
    }

    pub fn lpush(value: impl Into<Vec<u8>>) -> Self {
        Self::new(call::lpush(value.into()))
    }

    pub fn rpush(value: impl Into<Vec<u8>>) -> Self {
        Self::new(call::rpush(value.into()))
    }
}

impl From<BlobMutate> for MutateExpr {
    fn from(v: BlobMutate) -> Self {
        Expr::from(v.expr).into()
    }
}
