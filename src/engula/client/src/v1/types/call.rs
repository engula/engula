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

macro_rules! call {
    ($func:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![],
            ..Default::default()
        }
    };
    ($func:expr, $arg0:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![$arg0.into().into()],
            ..Default::default()
        }
    };
}

pub fn get() -> CallExpr {
    call!(Function::Get)
}

pub fn set(v: impl Into<Value>) -> CallExpr {
    call!(Function::Set, v)
}

pub fn delete() -> CallExpr {
    call!(Function::Delete)
}

pub fn exists() -> CallExpr {
    call!(Function::Exists)
}

pub fn add(v: impl Into<Value>) -> CallExpr {
    call!(Function::Add, v)
}

pub fn sub(v: impl Into<Value>) -> CallExpr {
    call!(Function::Sub, v)
}

pub fn trim(r: impl Into<Value>) -> CallExpr {
    call!(Function::Trim, r)
}

pub fn lpop(n: impl Into<Value>) -> CallExpr {
    call!(Function::Lpop, n)
}

pub fn rpop(n: impl Into<Value>) -> CallExpr {
    call!(Function::Rpop, n)
}

pub fn lpush(v: impl Into<Value>) -> CallExpr {
    call!(Function::Lpush, v)
}

pub fn rpush(v: impl Into<Value>) -> CallExpr {
    call!(Function::Rpush, v)
}

pub fn len() -> CallExpr {
    call!(Function::Len)
}

pub fn index(i: impl Into<Value>) -> CallExpr {
    call!(Function::Index, i)
}

pub fn range(r: impl Into<Value>) -> CallExpr {
    call!(Function::Range, r)
}

pub fn clear() -> CallExpr {
    call!(Function::Clear)
}

pub fn extend(v: impl Into<Value>) -> CallExpr {
    call!(Function::Extend, v)
}

pub fn remove(i: impl Into<Value>) -> CallExpr {
    call!(Function::Remove, i)
}

pub fn contains(i: impl Into<Value>) -> CallExpr {
    call!(Function::Contains, i)
}
