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

pub struct List {}

impl List {
    pub fn len() -> ListSelect {
        ListSelect::len()
    }

    pub fn range(range: impl RangeBounds<i64>) -> ListSelect {
        ListSelect::range(range)
    }

    pub fn element(index: i64) -> ListSelect {
        ListSelect::element(index)
    }

    pub fn elements(indexs: impl Into<Vec<i64>>) -> ListSelect {
        ListSelect::elements(indexs)
    }

    pub fn set(value: impl Into<ListValue>) -> ListMutate {
        ListMutate::set(value)
    }

    pub fn pop_back(count: i64) -> ListMutate {
        ListMutate::pop_back(count)
    }

    pub fn pop_front(count: i64) -> ListMutate {
        ListMutate::pop_front(count)
    }

    pub fn push_back(value: impl Into<ListValue>) -> ListMutate {
        ListMutate::push_back(value)
    }

    pub fn push_front(value: impl Into<ListValue>) -> ListMutate {
        ListMutate::push_front(value)
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

    pub fn range(range: impl RangeBounds<i64>) -> Self {
        Self::new(call::range(range))
    }

    pub fn element(index: i64) -> Self {
        Self::elements(vec![index])
    }

    pub fn elements(indexs: impl Into<Vec<i64>>) -> Self {
        Self::new(call::index(ListValue::from(indexs.into())))
    }
}

impl From<ListSelect> for SelectExpr {
    fn from(v: ListSelect) -> Self {
        TypedExpr::from(v.expr).into()
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

    pub fn set(value: impl Into<ListValue>) -> Self {
        Self::new(call::set(value.into()))
    }

    pub fn pop_back(count: i64) -> Self {
        Self::new(call::pop_back(count))
    }

    pub fn pop_front(count: i64) -> Self {
        Self::new(call::pop_front(count))
    }

    pub fn push_back(value: impl Into<ListValue>) -> Self {
        Self::new(call::push_back(value.into()))
    }

    pub fn push_front(value: impl Into<ListValue>) -> Self {
        Self::new(call::push_front(value.into()))
    }
}

impl From<ListMutate> for MutateExpr {
    fn from(v: ListMutate) -> Self {
        TypedExpr::from(v.expr).into()
    }
}
