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

pub struct List(ListValue);

impl From<List> for Value {
    fn from(v: List) -> Self {
        v.0.into()
    }
}

impl List {
    pub fn new(value: impl Into<ListValue>) -> Self {
        Self(value.into())
    }

    pub fn len() -> ListSelect {
        ListSelect::len()
    }

    pub fn index(index: impl Into<ListValue>) -> ListSelect {
        ListSelect::index(index)
    }

    pub fn range(range: impl RangeBounds<i64>) -> ListSelect {
        ListSelect::range(range)
    }

    pub fn trim(range: impl RangeBounds<i64>) -> ListMutate {
        ListMutate::trim(range)
    }

    pub fn lpop(count: i64) -> ListMutate {
        ListMutate::lpop(count)
    }

    pub fn rpop(count: i64) -> ListMutate {
        ListMutate::rpop(count)
    }

    pub fn lpush(value: impl Into<ListValue>) -> ListMutate {
        ListMutate::lpush(value)
    }

    pub fn rpush(value: impl Into<ListValue>) -> ListMutate {
        ListMutate::rpush(value)
    }
}

pub struct ListSelect {
    expr: ListExpr,
}

impl ListSelect {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: ListExpr { call: Some(call) },
        }
    }

    pub fn len() -> Self {
        Self::new(call::len())
    }

    pub fn index(index: impl Into<ListValue>) -> Self {
        Self::new(call::index(index.into()))
    }

    pub fn range(range: impl RangeBounds<i64>) -> Self {
        Self::new(call::range(range_bounds(range)))
    }
}

impl From<ListSelect> for SelectExpr {
    fn from(v: ListSelect) -> Self {
        Expr::from(v.expr).into()
    }
}

pub struct ListMutate {
    expr: ListExpr,
}

impl ListMutate {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: ListExpr { call: Some(call) },
        }
    }

    pub fn trim(range: impl RangeBounds<i64>) -> Self {
        Self::new(call::trim(range_bounds(range)))
    }

    pub fn lpop(count: i64) -> Self {
        Self::new(call::lpop(count))
    }

    pub fn rpop(count: i64) -> Self {
        Self::new(call::rpop(count))
    }

    pub fn lpush(value: impl Into<ListValue>) -> Self {
        Self::new(call::lpush(value.into()))
    }

    pub fn rpush(value: impl Into<ListValue>) -> Self {
        Self::new(call::rpush(value.into()))
    }
}

impl From<ListMutate> for MutateExpr {
    fn from(v: ListMutate) -> Self {
        Expr::from(v.expr).into()
    }
}
