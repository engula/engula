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

#[derive(Debug)]
pub enum Value {
    None,
    Blob(Vec<u8>),
    Text(String),
    Int64(i64),
}

impl Value {
    pub fn as_i64(self) -> Option<i64> {
        if let Value::Int64(v) = self {
            Some(v)
        } else {
            None
        }
    }
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

impl From<GenericValue> for Value {
    fn from(v: GenericValue) -> Self {
        if let Some(v) = v.value {
            match v {
                generic_value::Value::BlobValue(v) => Self::Blob(v),
                generic_value::Value::TextValue(v) => Self::Text(v),
                generic_value::Value::Int64Value(v) => Self::Int64(v),
                _ => todo!(),
            }
        } else {
            Self::None
        }
    }
}

impl From<Option<GenericValue>> for Value {
    fn from(v: Option<GenericValue>) -> Self {
        if let Some(v) = v {
            v.into()
        } else {
            Self::None
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
        };
        GenericValue { value }
    }
}
