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

use crate::{Any, Object, ObjectValue, Result, Txn};

pub struct I64(Any);

impl Object for I64 {
    type Txn = I64Txn;
    type Value = i64;
}

impl From<Any> for I64 {
    fn from(ob: Any) -> Self {
        Self(ob)
    }
}

impl I64 {
    pub fn begin(self) -> I64Txn {
        self.0.begin().into()
    }

    pub async fn load(self) -> Result<Option<i64>> {
        let value = self.0.load().await?;
        i64::cast_from_option(value)
    }

    pub async fn store(self, value: i64) -> Result<()> {
        self.0.store(value).await
    }

    pub async fn reset(self) -> Result<()> {
        self.0.reset().await
    }

    pub async fn add(self, value: i64) -> Result<()> {
        self.0.add(value).await
    }

    pub async fn sub(self, value: i64) -> Result<()> {
        self.0.sub(value).await
    }
}

pub struct I64Txn(Txn);

impl From<Txn> for I64Txn {
    fn from(txn: Txn) -> Self {
        Self(txn)
    }
}

impl I64Txn {
    pub fn store(&mut self, value: i64) -> &mut Self {
        self.0.store(value);
        self
    }

    pub fn reset(&mut self) -> &mut Self {
        self.0.reset();
        self
    }

    pub fn add(&mut self, value: i64) -> &mut Self {
        self.0.add(value);
        self
    }

    pub fn sub(&mut self, value: i64) -> &mut Self {
        self.0.sub(value);
        self
    }

    pub async fn commit(self) -> Result<()> {
        self.0.commit().await
    }
}
