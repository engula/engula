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

use crate::{Object, Result, TypedObject, TypedValue};

pub struct List<T> {
    _ob: Object,
    _marker: PhantomData<T>,
}

impl<T> From<Object> for List<T> {
    fn from(ob: Object) -> Self {
        Self {
            _ob: ob,
            _marker: PhantomData,
        }
    }
}

impl<T> TypedObject for List<T>
where
    T: TypedObject,
    Vec<T::TypedValue>: TypedValue,
{
    type TypedValue = Vec<T::TypedValue>;
}

impl<T> List<T>
where
    T: TypedObject,
    Vec<T::TypedValue>: TypedValue,
{
    pub async fn get(&self, _index: i64) -> Result<Option<T::TypedValue>> {
        todo!();
    }

    pub async fn set(&self, _index: i64, _value: impl Into<T::TypedValue>) -> Result<()> {
        todo!();
    }

    pub async fn delete(&self, _index: i64) -> Result<()> {
        todo!();
    }
}
