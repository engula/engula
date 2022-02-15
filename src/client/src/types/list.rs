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

use std::marker::PhantomData;

use crate::{Any, Object, ObjectValue, Result, Txn};

pub struct List<T> {
    ob: Any,
    _marker: PhantomData<T>,
}

impl<T> From<Any> for List<T> {
    fn from(ob: Any) -> Self {
        Self {
            ob,
            _marker: PhantomData,
        }
    }
}

impl<T> Object for List<T>
where
    T: Object,
    Vec<T::Value>: ObjectValue,
{
    type Txn = ListTxn<T>;
    type Value = Vec<T::Value>;
}

impl<T> List<T>
where
    T: Object,
    Vec<T::Value>: ObjectValue,
{
    pub fn begin(self) -> ListTxn<T> {
        self.ob.begin().into()
    }

    pub async fn len(self) -> Result<i64> {
        self.ob.len().await
    }

    pub async fn get(self, index: i64) -> Result<Option<T::Value>> {
        let value = self.ob.get(index).await?;
        T::Value::cast_from_option(value)
    }

    pub async fn set(self, index: i64, value: impl Into<T::Value>) -> Result<()> {
        self.ob.set(index, value.into()).await
    }

    pub async fn pop_back(self) -> Result<Option<T::Value>> {
        let value = self.ob.pop_back().await?;
        T::Value::cast_from_option(value)
    }

    pub async fn pop_front(self) -> Result<Option<T::Value>> {
        let value = self.ob.pop_front().await?;
        T::Value::cast_from_option(value)
    }

    pub async fn push_back(self, value: impl Into<T::Value>) -> Result<()> {
        self.ob.push_back(value.into()).await
    }

    pub async fn push_front(self, value: impl Into<T::Value>) -> Result<()> {
        self.ob.push_front(value.into()).await
    }
}

pub struct ListTxn<T> {
    txn: Txn,
    _marker: PhantomData<T>,
}

impl<T> From<Txn> for ListTxn<T> {
    fn from(txn: Txn) -> Self {
        Self {
            txn,
            _marker: PhantomData,
        }
    }
}

impl<T: Object> ListTxn<T> {
    pub fn set(&mut self, index: i64, value: impl Into<T::Value>) -> &mut Self {
        self.txn.set(index, value.into());
        self
    }

    pub fn push_back(&mut self, value: impl Into<T::Value>) -> &mut Self {
        self.txn.push_back(value.into());
        self
    }

    pub fn push_front(&mut self, value: impl Into<T::Value>) -> &mut Self {
        self.txn.push_front(value.into());
        self
    }

    pub async fn commit(self) -> Result<()> {
        self.txn.commit().await
    }
}
