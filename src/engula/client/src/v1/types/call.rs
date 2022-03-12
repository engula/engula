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

macro_rules! range_call {
    ($func:expr, $range:expr) => {
        CallExpr {
            func: $func as i32,
            args: vec![],
            range: Some($range.into()),
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

pub fn add(v: impl Into<Value>) -> CallExpr {
    call!(Function::Add, v)
}

pub fn sub(v: impl Into<Value>) -> CallExpr {
    call!(Function::Sub, v)
}

pub fn pop_back(n: impl Into<Value>) -> CallExpr {
    call!(Function::PopBack, n)
}

pub fn pop_front(n: impl Into<Value>) -> CallExpr {
    call!(Function::PopFront, n)
}

pub fn push_back(v: impl Into<Value>) -> CallExpr {
    call!(Function::PushBack, v)
}

pub fn push_front(v: impl Into<Value>) -> CallExpr {
    call!(Function::PushFront, v)
}

pub fn len() -> CallExpr {
    call!(Function::Len)
}

pub fn index(index: impl Into<Value>) -> CallExpr {
    call!(Function::Index, index)
}

pub fn range(range: impl Into<RangeExpr>) -> CallExpr {
    range_call!(Function::Range, range)
}
