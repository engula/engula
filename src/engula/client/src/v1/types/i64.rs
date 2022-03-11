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

use super::numeric;

pub struct I64 {}

impl I64 {
    pub fn set(v: i64) -> I64Mutate {
        I64Mutate::set(v)
    }

    pub fn add(v: i64) -> I64Mutate {
        I64Mutate::add(v)
    }

    pub fn sub(v: i64) -> I64Mutate {
        I64Mutate::sub(v)
    }
}

pub struct I64Mutate {
    expr: I64MutateExpr,
}

impl I64Mutate {
    fn new(call: NumericCallExpr) -> Self {
        Self {
            expr: I64MutateExpr { call: Some(call) },
        }
    }

    pub fn set(v: i64) -> Self {
        Self::new(numeric::set(v))
    }

    pub fn add(v: i64) -> Self {
        Self::new(numeric::add(v))
    }

    pub fn sub(v: i64) -> Self {
        Self::new(numeric::sub(v))
    }
}

impl From<I64Mutate> for MutateExpr {
    fn from(v: I64Mutate) -> Self {
        Self {
            expr: Some(mutate_expr::Expr::I64Expr(v.expr)),
        }
    }
}
