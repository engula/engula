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

pub struct F64(f64);

impl From<F64> for Value {
    fn from(v: F64) -> Self {
        v.0.into()
    }
}

impl F64 {
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    pub fn add(value: f64) -> F64Mutate {
        F64Mutate::add(value)
    }

    pub fn sub(value: f64) -> F64Mutate {
        F64Mutate::sub(value)
    }
}

pub struct F64Mutate {
    expr: F64Expr,
}

impl F64Mutate {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: F64Expr { call: Some(call) },
        }
    }

    pub fn add(value: f64) -> Self {
        Self::new(call::add(value))
    }

    pub fn sub(value: f64) -> Self {
        Self::new(call::sub(value))
    }
}

impl From<F64Mutate> for MutateExpr {
    fn from(v: F64Mutate) -> Self {
        Expr::from(v.expr).into()
    }
}
