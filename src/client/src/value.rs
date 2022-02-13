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

use crate::{Error, Result};

#[derive(Debug)]
pub enum Value {
    None,
    Blob(Vec<u8>),
    Text(String),
    Int64(i64),
    List(ListValue),
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Self::Blob(v.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::Blob(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::Text(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int64(v)
    }
}

impl From<Vec<Vec<u8>>> for Value {
    fn from(v: Vec<Vec<u8>>) -> Self {
        Self::List(ListValue::Blob(v))
    }
}

impl From<Vec<String>> for Value {
    fn from(v: Vec<String>) -> Self {
        Self::List(ListValue::Text(v))
    }
}

impl From<Vec<i64>> for Value {
    fn from(v: Vec<i64>) -> Self {
        Self::List(ListValue::Int64(v))
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Self::List(ListValue::Value(v))
    }
}

impl From<GenericValue> for Value {
    fn from(v: GenericValue) -> Self {
        v.value.map(|v| v.into()).unwrap_or(Self::None)
    }
}

impl From<generic_value::Value> for Value {
    fn from(v: generic_value::Value) -> Self {
        match v {
            generic_value::Value::BlobValue(v) => Self::Blob(v),
            generic_value::Value::TextValue(v) => Self::Text(v),
            generic_value::Value::Int64Value(v) => Self::Int64(v),
            generic_value::Value::RepeatedValue(v) => {
                let value = if !v.blob_values.is_empty() {
                    ListValue::Blob(v.blob_values)
                } else if !v.text_values.is_empty() {
                    ListValue::Text(v.text_values)
                } else if !v.int64_values.is_empty() {
                    ListValue::Int64(v.int64_values)
                } else {
                    ListValue::Value(v.values.into_iter().map(|x| x.into()).collect())
                };
                Self::List(value)
            }
            _ => todo!(),
        }
    }
}

impl From<Value> for GenericValue {
    fn from(v: Value) -> GenericValue {
        let value = match v {
            Value::None => None,
            Value::Blob(v) => Some(generic_value::Value::BlobValue(v)),
            Value::Text(v) => Some(generic_value::Value::TextValue(v)),
            Value::Int64(v) => Some(generic_value::Value::Int64Value(v)),
            Value::List(v) => {
                let mut value = RepeatedValue::default();
                match v {
                    ListValue::Blob(v) => value.blob_values = v,
                    ListValue::Text(v) => value.text_values = v,
                    ListValue::Int64(v) => value.int64_values = v,
                    ListValue::Value(v) => value.values = v.into_iter().map(|x| x.into()).collect(),
                }
                Some(generic_value::Value::RepeatedValue(value))
            }
        };
        GenericValue { value }
    }
}

#[derive(Debug)]
pub enum ListValue {
    Blob(Vec<Vec<u8>>),
    Text(Vec<String>),
    Int64(Vec<i64>),
    Value(Vec<Value>),
}

impl From<Vec<Vec<u8>>> for ListValue {
    fn from(v: Vec<Vec<u8>>) -> Self {
        Self::Blob(v)
    }
}

impl From<Vec<String>> for ListValue {
    fn from(v: Vec<String>) -> Self {
        Self::Text(v)
    }
}

impl From<Vec<i64>> for ListValue {
    fn from(v: Vec<i64>) -> Self {
        Self::Int64(v)
    }
}

impl From<Vec<Value>> for ListValue {
    fn from(v: Vec<Value>) -> Self {
        Self::Value(v)
    }
}

impl TryFrom<ListValue> for Vec<Vec<u8>> {
    type Error = Error;

    fn try_from(v: ListValue) -> Result<Self> {
        match v {
            ListValue::Blob(v) => Ok(v),
            _ => todo!(),
        }
    }
}

impl TryFrom<ListValue> for Vec<String> {
    type Error = Error;

    fn try_from(v: ListValue) -> Result<Self> {
        match v {
            ListValue::Text(v) => Ok(v),
            _ => todo!(),
        }
    }
}

impl TryFrom<ListValue> for Vec<i64> {
    type Error = Error;

    fn try_from(v: ListValue) -> Result<Self> {
        match v {
            ListValue::Int64(v) => Ok(v),
            _ => todo!(),
        }
    }
}

impl TryFrom<ListValue> for Vec<Value> {
    type Error = Error;

    fn try_from(v: ListValue) -> Result<Self> {
        match v {
            ListValue::Value(v) => Ok(v),
            _ => todo!(),
        }
    }
}

pub trait TypedValue: Into<Value> {
    fn cast_from(v: Value) -> Result<Self>;
}

impl TypedValue for Value {
    fn cast_from(v: Value) -> Result<Self> {
        Ok(v)
    }
}

impl TypedValue for i64 {
    fn cast_from(v: Value) -> Result<Self> {
        if let Value::Int64(v) = v {
            Ok(v)
        } else {
            Err(Error::TypeMismatch)
        }
    }
}

pub trait TypedListValue: Into<Value> + TryFrom<ListValue, Error = Error> {}

impl<T: TryFrom<ListValue, Error = Error> + Into<Value>> TypedListValue for T {}

impl<T: TypedListValue> TypedValue for T {
    fn cast_from(v: Value) -> Result<Self> {
        if let Value::List(v) = v {
            v.try_into()
        } else {
            Err(Error::TypeMismatch)
        }
    }
}
