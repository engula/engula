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

use crate::{
    expr::{call_expr, subcall_expr},
    Error, Object, Result, TypedObject, TypedValue,
};

pub struct List<T> {
    ob: Object,
    _marker: PhantomData<T>,
}

impl<T> From<Object> for List<T> {
    fn from(ob: Object) -> Self {
        Self {
            ob,
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
    pub async fn len(self) -> Result<i64> {
        let value = self.ob.call(call_expr::len()).await?;
        if let Some(v) = value {
            v.as_i64().ok_or(Error::TypeMismatch)
        } else {
            Ok(0)
        }
    }

    pub async fn pop(self) -> Result<Option<T::TypedValue>> {
        let value = self.ob.call(call_expr::pop()).await?;
        if let Some(v) = value {
            let v = T::TypedValue::cast_from(v)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub async fn push(self, value: impl Into<T::TypedValue>) -> Result<()> {
        self.ob.call(call_expr::push(value.into())).await?;
        Ok(())
    }

    pub async fn get(self, index: i64) -> Result<Option<T::TypedValue>> {
        let value = self.ob.subcall(subcall_expr::get(index)).await?;
        if let Some(v) = value {
            let v = T::TypedValue::cast_from(v)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub async fn set(self, index: i64, value: impl Into<T::TypedValue>) -> Result<()> {
        self.ob
            .subcall(subcall_expr::set(index, value.into()))
            .await?;
        Ok(())
    }

    pub async fn delete(self, index: i64) -> Result<()> {
        self.ob.subcall(subcall_expr::delete(index)).await?;
        Ok(())
    }
}
