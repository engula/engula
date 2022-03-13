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

use std::ops::{Bound, RangeBounds};

use engula_apis::v1::*;

macro_rules! call {
    ($func:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![],
            ..Default::default()
        }
    };
    ($func:expr, $arg0:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![$arg0.into()],
            ..Default::default()
        }
    };
}

macro_rules! index_call {
    ($func:expr, $index:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![],
            operand: Some(call_expr::Operand::Index($index.into())),
        }
    };
}

macro_rules! range_call {
    ($func:expr, $range:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![],
            operand: Some(call_expr::Operand::Range($range.into())),
        }
    };
}

pub fn get() -> CallExpr {
    call!(Function::Get)
}

pub fn get_index(index: impl Into<TypedValue>) -> CallExpr {
    index_call!(Function::Get, index)
}

pub fn get_range(range: impl Into<TypedRange>) -> CallExpr {
    range_call!(Function::Get, range)
}

pub fn set(v: impl Into<TypedValue>) -> CallExpr {
    call!(Function::Set, v)
}

pub fn delete() -> CallExpr {
    call!(Function::Delete)
}

pub fn delete_index(index: impl Into<TypedValue>) -> CallExpr {
    index_call!(Function::Delete, index)
}

pub fn add(v: impl Into<TypedValue>) -> CallExpr {
    call!(Function::Add, v)
}

pub fn sub(v: impl Into<TypedValue>) -> CallExpr {
    call!(Function::Sub, v)
}

pub fn pop_back(n: impl Into<TypedValue>) -> CallExpr {
    call!(Function::PopBack, n)
}

pub fn pop_front(n: impl Into<TypedValue>) -> CallExpr {
    call!(Function::PopFront, n)
}

pub fn push_back(v: impl Into<TypedValue>) -> CallExpr {
    call!(Function::PushBack, v)
}

pub fn push_front(v: impl Into<TypedValue>) -> CallExpr {
    call!(Function::PushFront, v)
}

pub fn len() -> CallExpr {
    call!(Function::Len)
}

pub fn extend(value: impl Into<TypedValue>) -> CallExpr {
    call!(Function::Extend, value)
}

pub fn range<T>(range: impl RangeBounds<T>) -> TypedRange
where
    T: Clone + Into<TypedValue>,
{
    let mut expr = TypedRange::default();
    match range.start_bound().cloned() {
        Bound::Included(start) => {
            expr.start = Some(start.into());
            expr.start_bound = RangeBound::Included as i32;
        }
        Bound::Excluded(start) => {
            expr.start = Some(start.into());
            expr.start_bound = RangeBound::Excluded as i32;
        }
        Bound::Unbounded => {
            expr.start_bound = RangeBound::Unbounded as i32;
        }
    }
    match range.end_bound().cloned() {
        Bound::Included(end) => {
            expr.end = Some(end.into());
            expr.end_bound = RangeBound::Included as i32;
        }
        Bound::Excluded(end) => {
            expr.end = Some(end.into());
            expr.end_bound = RangeBound::Excluded as i32;
        }
        Bound::Unbounded => {
            expr.end_bound = RangeBound::Unbounded as i32;
        }
    }
    expr
}
