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

use engula_apis::*;

macro_rules! call_expr {
    ($func:expr, $args:expr) => {
        CallExpr {
            func: $func as i32,
            args: $args,
        }
    };
}

pub fn get() -> CallExpr {
    call_expr!(Function::Get, vec![])
}

pub fn set(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Set, vec![value.into()])
}

pub fn delete() -> CallExpr {
    call_expr!(Function::Delete, vec![])
}

pub fn add_assign(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::AddAssign, vec![value.into()])
}

pub fn sub_assign(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::SubAssign, vec![value.into()])
}

pub fn len() -> CallExpr {
    call_expr!(Function::Len, vec![])
}

pub fn pop() -> CallExpr {
    call_expr!(Function::Pop, vec![])
}

pub fn push(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Push, vec![value.into()])
}
