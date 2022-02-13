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

pub fn get(id: impl Into<GenericValue>) -> CallExpr {
    CallExpr {
        func: Function::Get as i32,
        args: vec![id.into()],
    }
}

pub fn set(id: impl Into<GenericValue>, value: impl Into<GenericValue>) -> CallExpr {
    CallExpr {
        func: Function::Set as i32,
        args: vec![id.into(), value.into()],
    }
}

pub fn delete(id: impl Into<GenericValue>) -> CallExpr {
    CallExpr {
        func: Function::Delete as i32,
        args: vec![id.into()],
    }
}
