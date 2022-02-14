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

use crate::{Error, Object, Result};

pub trait TypedObject: From<Object> {
    type TypedValue: TypedValue;
}

impl TypedObject for Object {
    type TypedValue = Value;
}

pub trait TypedValue: Into<Value> {
    fn cast_from(v: Value) -> Result<Self>;
}

impl TypedValue for Value {
    fn cast_from(v: Value) -> Result<Self> {
        Ok(v)
    }
}

impl TypedValue for Vec<Value> {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::ListValue(v)) = v.value {
            Ok(v.values)
        } else {
            Err(Error::TypeMismatch)
        }
    }
}

impl TypedValue for i64 {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::Int64Value(v)) = v.value {
            Ok(v)
        } else {
            Err(Error::TypeMismatch)
        }
    }
}

impl TypedValue for Vec<i64> {
    fn cast_from(v: Value) -> Result<Self> {
        if let Some(value::Value::ListValue(v)) = v.value {
            v.values.into_iter().map(i64::cast_from).collect()
        } else {
            Err(Error::TypeMismatch)
        }
    }
}
