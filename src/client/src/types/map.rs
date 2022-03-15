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

use std::{collections::HashMap, marker::PhantomData};

use crate::{Any, Object, ObjectValue, Result, Txn};

pub struct Map<T> {
    ob: Any,
    _marker: PhantomData<T>,
}

impl<T> From<Any> for Map<T> {
    fn from(ob: Any) -> Self {
        Self {
            ob,
            _marker: PhantomData,
        }
    }
}

impl<T> Object for Map<T>
where
    T: Object,
    HashMap<Vec<u8>, T::Value>: ObjectValue,
{
    type Txn = MapTxn<T>;
    type Value = HashMap<Vec<u8>, T::Value>;
}

impl<T> Map<T>
where
    T: Object,
    HashMap<Vec<u8>, T::Value>: ObjectValue,
{
    pub fn begin(self) -> MapTxn<T> {
        self.ob.begin().into()
    }

    pub async fn load(self) -> Result<Option<HashMap<Vec<u8>, T::Value>>> {
        let value = self.ob.load().await?;
        HashMap::cast_from_option(value)
    }

    pub async fn store(self, value: impl Into<HashMap<Vec<u8>, T::Value>>) -> Result<()> {
        self.ob.store(value.into()).await
    }

    pub async fn reset(self) -> Result<()> {
        self.ob.reset().await
    }

    pub async fn len(self) -> Result<Option<i64>> {
        self.ob.len().await
    }

    pub async fn get(self, key: impl Into<Vec<u8>>) -> Result<Option<T::Value>> {
        let value = self.ob.index(key.into()).load().await?;
        T::Value::cast_from_option(value)
    }

    pub async fn set(self, key: impl Into<Vec<u8>>, value: impl Into<T::Value>) -> Result<()> {
        self.ob.index(key.into()).store(value.into()).await
    }

    pub async fn delete(self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.ob.index(key.into()).reset().await
    }
}

pub struct MapTxn<T> {
    txn: Txn,
    _marker: PhantomData<T>,
}

impl<T> From<Txn> for MapTxn<T> {
    fn from(txn: Txn) -> Self {
        Self {
            txn,
            _marker: PhantomData,
        }
    }
}

impl<T: Object> MapTxn<T> {
    pub fn store(&mut self, value: impl Into<HashMap<Vec<u8>, T::Value>>) -> &mut Self {
        self.txn.store(value.into());
        self
    }

    pub fn reset(&mut self) -> &mut Self {
        self.txn.reset();
        self
    }

    pub fn set(&mut self, key: impl Into<Vec<u8>>, value: impl Into<T::Value>) -> &mut Self {
        self.txn.set(key.into(), value.into());
        self
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.txn.delete(key.into());
        self
    }

    pub async fn commit(self) -> Result<()> {
        self.txn.commit().await
    }
}
