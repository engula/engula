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

pub fn get(index: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Get, vec![index.into()])
}

pub fn set(index: impl Into<Value>, value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Set, vec![index.into(), value.into()])
}

pub fn remove(index: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Delete, vec![index.into()])
}
