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

pub struct Set(SetValue);

impl From<Set> for Value {
    fn from(v: Set) -> Self {
        v.0.into()
    }
}

impl Set {
    pub fn new(value: impl Into<SetValue>) -> Self {
        Self(value.into())
    }

    pub fn len() -> SetSelect {
        SetSelect::len()
    }

    pub fn range<T>(range: impl RangeBounds<T>) -> SetSelect
    where
        T: Clone + Into<range_bound::Value>,
    {
        SetSelect::range(range)
    }

    pub fn contains(index: impl Into<ListValue>) -> SetSelect {
        SetSelect::contains(index)
    }

    pub fn clear() -> SetMutate {
        SetMutate::clear()
    }

    pub fn extend(value: impl Into<SetValue>) -> SetMutate {
        SetMutate::extend(value)
    }

    pub fn remove(index: impl Into<ListValue>) -> SetMutate {
        SetMutate::remove(index)
    }
}

pub struct SetSelect {
    expr: SetExpr,
}

impl SetSelect {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: SetExpr { call: Some(call) },
        }
    }

    pub fn len() -> Self {
        Self::new(call::len())
    }

    pub fn range<T>(range: impl RangeBounds<T>) -> Self
    where
        T: Clone + Into<range_bound::Value>,
    {
        Self::new(call::range(range_bounds(range)))
    }

    pub fn contains(index: impl Into<ListValue>) -> Self {
        Self::new(call::contains(index.into()))
    }
}

impl From<SetSelect> for SelectExpr {
    fn from(v: SetSelect) -> Self {
        Expr::from(v.expr).into()
    }
}

pub struct SetMutate {
    expr: SetExpr,
}

impl SetMutate {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: SetExpr { call: Some(call) },
        }
    }

    pub fn clear() -> Self {
        Self::new(call::clear())
    }

    pub fn extend(value: impl Into<SetValue>) -> Self {
        Self::new(call::extend(value.into()))
    }

    pub fn remove(index: impl Into<ListValue>) -> Self {
        Self::new(call::remove(index.into()))
    }
}

impl From<SetMutate> for MutateExpr {
    fn from(v: SetMutate) -> Self {
        Expr::from(v.expr).into()
    }
}
