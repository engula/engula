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

macro_rules! simple_expr {
    ($id:ident, $func:expr, $args:expr) => {
        Expr {
            id: $id.into(),
            call: Some(CallExpr {
                func: $func as i32,
                args: $args,
            }),
        }
    };
}

pub fn get(id: impl Into<Vec<u8>>) -> Expr {
    simple_expr!(id, Function::Get, vec![])
}

pub fn set(id: impl Into<Vec<u8>>, value: impl Into<GenericValue>) -> Expr {
    simple_expr!(id, Function::Set, vec![value.into()])
}

pub fn delete(id: impl Into<Vec<u8>>) -> Expr {
    simple_expr!(id, Function::Delete, vec![])
}
