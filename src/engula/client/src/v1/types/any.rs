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

pub struct Any {}

impl Any {
    pub fn get() -> AnySelect {
        AnySelect::get()
    }

    pub fn exists() -> AnySelect {
        AnySelect::exists()
    }

    pub fn set(value: impl Into<Value>) -> AnyMutate {
        AnyMutate::set(value)
    }

    pub fn delete() -> AnyMutate {
        AnyMutate::delete()
    }
}

pub struct AnySelect {
    expr: AnyExpr,
}

impl AnySelect {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: AnyExpr { call: Some(call) },
        }
    }

    pub fn get() -> Self {
        Self::new(call::get())
    }

    pub fn exists() -> Self {
        Self::new(call::exists())
    }
}

impl From<AnySelect> for SelectExpr {
    fn from(v: AnySelect) -> Self {
        Expr::from(v.expr).into()
    }
}

pub struct AnyMutate {
    expr: AnyExpr,
}

impl AnyMutate {
    fn new(call: CallExpr) -> Self {
        Self {
            expr: AnyExpr { call: Some(call) },
        }
    }

    pub fn set(value: impl Into<Value>) -> Self {
        Self::new(call::set(value))
    }

    pub fn delete() -> Self {
        Self::new(call::delete())
    }
}

impl From<AnyMutate> for MutateExpr {
    fn from(v: AnyMutate) -> Self {
        Expr::from(v.expr).into()
    }
}
