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

use crate::{
    expr::{call_expr, subcall_expr},
    Any, Error, Object, ObjectValue, Result,
};

pub struct UnorderedMap<T> {
    ob: Any,
    _marker: PhantomData<T>,
}

impl<T> From<Any> for UnorderedMap<T> {
    fn from(ob: Any) -> Self {
        Self {
            ob,
            _marker: PhantomData,
        }
    }
}

impl<T> Object for UnorderedMap<T>
where
    T: Object,
    HashMap<Vec<u8>, T::Value>: ObjectValue,
{
    type Value = HashMap<Vec<u8>, T::Value>;
}

impl<T> UnorderedMap<T>
where
    T: Object,
    HashMap<Vec<u8>, T::Value>: ObjectValue,
{
    pub async fn len(self) -> Result<i64> {
        let value = self.ob.call(call_expr::len()).await?;
        if let Some(v) = value {
            v.as_i64().ok_or(Error::TypeMismatch)
        } else {
            Ok(0)
        }
    }

    pub async fn get(self, key: impl Into<Vec<u8>>) -> Result<Option<T::Value>> {
        let value = self.ob.subcall(subcall_expr::get(key.into())).await?;
        if let Some(v) = value {
            let v = T::Value::cast_from(v)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub async fn set(self, key: impl Into<Vec<u8>>, value: impl Into<T::Value>) -> Result<()> {
        self.ob
            .subcall(subcall_expr::set(key.into(), value.into()))
            .await?;
        Ok(())
    }

    pub async fn delete(self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.ob.subcall(subcall_expr::delete(key.into())).await?;
        Ok(())
    }
}
