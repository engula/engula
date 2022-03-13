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

use super::{call, MutateExpr};

pub struct I64(i64);

impl From<I64> for Value {
    fn from(v: I64) -> Self {
        v.0.into()
    }
}

impl I64 {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn add(value: i64) -> I64Mutate {
        I64Mutate::add(value)
    }

    pub fn sub(value: i64) -> I64Mutate {
        I64Mutate::sub(value)
    }
}

pub struct I64Mutate {
    expr: I64Expr,
}

impl I64Mutate {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: I64Expr { call: Some(call) },
        }
    }

    pub fn add(value: i64) -> Self {
        Self::new(call::add(value))
    }

    pub fn sub(value: i64) -> Self {
        Self::new(call::sub(value))
    }
}

impl From<I64Mutate> for MutateExpr {
    fn from(v: I64Mutate) -> Self {
        Expr::from(v.expr).into()
    }
}
