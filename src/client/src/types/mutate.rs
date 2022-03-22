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

#[derive(Default)]
pub struct Mutate(MutateExpr);

impl Mutate {
    pub fn index(mut self, index: impl Into<Value>) -> Self {
        self.0.index = Some(index.into());
        self
    }

    pub fn set(mut self, v: impl Into<Value>) -> Mutate {
        self.0.func = MutateFunction::Set as i32;
        self.0.args = vec![v.into()];
        self
    }

    pub fn delete(mut self) -> Self {
        self.0.func = MutateFunction::Delete as i32;
        self
    }

    pub fn add(mut self, v: impl Into<Value>) -> Self {
        self.0.func = MutateFunction::Add as i32;
        self.0.args = vec![v.into()];
        self
    }

    pub fn trim(mut self, index: impl Into<Value>) -> Self {
        self.0.func = MutateFunction::Trim as i32;
        self.0.index = Some(index.into());
        self
    }

    pub fn lpop(mut self, count: i64) -> Self {
        self.0.func = MutateFunction::Lpop as i32;
        self.0.args = vec![count.into()];
        self
    }

    pub fn rpop(mut self, count: i64) -> Self {
        self.0.func = MutateFunction::Rpop as i32;
        self.0.args = vec![count.into()];
        self
    }

    pub fn lpush(mut self, value: impl Into<Value>) -> Self {
        self.0.func = MutateFunction::Lpush as i32;
        self.0.args = vec![value.into()];
        self
    }

    pub fn rpush(mut self, value: impl Into<Value>) -> Self {
        self.0.func = MutateFunction::Rpush as i32;
        self.0.args = vec![value.into()];
        self
    }

    pub fn clear(mut self) -> Self {
        self.0.func = MutateFunction::Clear as i32;
        self
    }

    pub fn extend(mut self, value: impl Into<Value>) -> Self {
        self.0.func = MutateFunction::Extend as i32;
        self.0.args = vec![value.into()];
        self
    }

    pub fn remove(mut self, index: impl Into<Value>) -> Self {
        self.0.func = MutateFunction::Remove as i32;
        self.0.index = Some(index.into());
        self
    }
}

impl From<Mutate> for MutateExpr {
    fn from(v: Mutate) -> Self {
        v.0
    }
}
