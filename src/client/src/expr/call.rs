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
    ($func:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![],
        }
    };
    ($func:expr, $arg0:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![$arg0],
        }
    };
}

pub fn load() -> CallExpr {
    call_expr!(Function::Load)
}

pub fn store(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Store, value.into().into())
}

pub fn reset() -> CallExpr {
    call_expr!(Function::Reset)
}

pub fn add(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Add, value.into().into())
}

pub fn sub(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Sub, value.into().into())
}

pub fn len() -> CallExpr {
    call_expr!(Function::Len)
}

pub fn append(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::Append, value.into().into())
}

pub fn push_back(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::PushBack, value.into().into())
}

pub fn push_front(value: impl Into<Value>) -> CallExpr {
    call_expr!(Function::PushFront, value.into().into())
}
