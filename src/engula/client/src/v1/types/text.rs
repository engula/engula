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

pub struct Text(String);

impl From<Text> for Value {
    fn from(v: Text) -> Self {
        v.0.into()
    }
}

impl Text {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn len() -> TextSelect {
        TextSelect::len()
    }

    pub fn range(range: impl RangeBounds<i64>) -> TextSelect {
        TextSelect::range(range)
    }

    pub fn trim(range: impl RangeBounds<i64>) -> TextMutate {
        TextMutate::trim(range)
    }

    pub fn lpop(count: i64) -> TextMutate {
        TextMutate::lpop(count)
    }

    pub fn rpop(count: i64) -> TextMutate {
        TextMutate::rpop(count)
    }

    pub fn lpush(value: impl Into<String>) -> TextMutate {
        TextMutate::lpush(value)
    }

    pub fn rpush(value: impl Into<String>) -> TextMutate {
        TextMutate::rpush(value)
    }
}

pub struct TextSelect {
    expr: TextExpr,
}

impl TextSelect {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: TextExpr { call: Some(call) },
        }
    }

    pub fn len() -> Self {
        Self::new(call::len())
    }

    pub fn range(range: impl RangeBounds<i64>) -> Self {
        Self::new(call::get_range(call::range(range)))
    }
}

impl From<TextSelect> for SelectExpr {
    fn from(v: TextSelect) -> Self {
        Expr::from(v.expr).into()
    }
}

pub struct TextMutate {
    expr: TextExpr,
}

impl TextMutate {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: TextExpr { call: Some(call) },
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

    pub fn lpush(value: impl Into<String>) -> Self {
        Self::new(call::lpush(value.into()))
    }

    pub fn rpush(value: impl Into<String>) -> Self {
        Self::new(call::rpush(value.into()))
    }
}

impl From<TextMutate> for MutateExpr {
    fn from(v: TextMutate) -> Self {
        Expr::from(v.expr).into()
    }
}
