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

use std::collections::HashMap;

use engula_apis::*;

use crate::{Any, Error, Result};

pub trait Object: From<Any> {
    type Value: ObjectValue;
}

impl Object for Any {
    type Value = Value;
}

pub trait ObjectValue: Into<Value> {
    fn cast_from(v: Value) -> Result<Self>;

    fn cast_from_option(v: Option<Value>) -> Result<Option<Self>> {
        if let Some(v) = v {
            Ok(Some(Self::cast_from(v)?))
        } else {
            Ok(None)
        }
    }
}

impl ObjectValue for Value {
    fn cast_from(v: Value) -> Result<Self> {
        Ok(v)
    }
}

impl ObjectValue for Vec<u8> {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::BlobValue(v)) = v.value {
            Ok(v)
        } else {
            Err(Error::TypeMismatch)
        }
    }
}

impl ObjectValue for String {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::TextValue(v)) = v.value {
            Ok(v)
        } else {
            Err(Error::TypeMismatch)
        }
    }
}

impl ObjectValue for i64 {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::Int64Value(v)) = v.value {
            Ok(v)
        } else {
            Err(Error::TypeMismatch)
        }
    }
}

impl<T: ObjectValue> ObjectValue for Vec<T> {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::SequenceValue(v)) = v.value {
            v.values.into_iter().map(T::cast_from).collect()
        } else {
            Err(Error::TypeMismatch)
        }
    }
}

impl<T: ObjectValue> ObjectValue for HashMap<Vec<u8>, T> {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::AssociativeValue(v)) = v.value {
            let result: Result<Vec<T>> = v.values.into_iter().map(T::cast_from).collect();
            result.map(|values| v.keys.into_iter().zip(values).collect())
        } else {
            Err(Error::TypeMismatch)
        }
    }
}
