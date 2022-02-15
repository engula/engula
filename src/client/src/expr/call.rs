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

pub fn call() -> Builder {
    Builder::default()
}

#[derive(Default)]
pub struct Builder(CallExpr);

impl Builder {
    pub fn add(mut self, value: impl Into<Value>) -> CallExpr {
        self.0.func = Function::Add as i32;
        self.0.args = vec![value.into()];
        self.0
    }

    pub fn sub(mut self, value: impl Into<Value>) -> CallExpr {
        self.0.func = Function::Sub as i32;
        self.0.args = vec![value.into()];
        self.0
    }

    pub fn len(mut self) -> CallExpr {
        self.0.func = Function::Len as i32;
        self.0
    }

    pub fn append(mut self, value: impl Into<Value>) -> CallExpr {
        self.0.func = Function::Append as i32;
        self.0.args = vec![value.into()];
        self.0
    }

    pub fn pop_back(mut self) -> CallExpr {
        self.0.func = Function::PopBack as i32;
        self.0
    }

    pub fn pop_front(mut self) -> CallExpr {
        self.0.func = Function::PopFront as i32;
        self.0
    }

    pub fn push_back(mut self, value: impl Into<Value>) -> CallExpr {
        self.0.func = Function::PushBack as i32;
        self.0.args = vec![value.into()];
        self.0
    }

    pub fn push_front(mut self, value: impl Into<Value>) -> CallExpr {
        self.0.func = Function::PushFront as i32;
        self.0.args = vec![value.into()];
        self.0
    }

    pub fn get(mut self, index: impl Into<Value>) -> CallExpr {
        self.0.func = Function::Get as i32;
        self.0.args = vec![index.into()];
        self.0
    }

    pub fn set(mut self, index: impl Into<Value>, value: impl Into<Value>) -> CallExpr {
        self.0.func = Function::Set as i32;
        self.0.args = vec![index.into(), value.into()];
        self.0
    }

    pub fn remove(mut self, index: impl Into<Value>) -> CallExpr {
        self.0.func = Function::Remove as i32;
        self.0.args = vec![index.into()];
        self.0
    }
}
