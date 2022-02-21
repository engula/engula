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

use std::{collections::HashMap, hash::Hash};

use engula_apis::*;

use crate::{Any, Error, Result, Txn};

pub trait Object: From<Any> {
    type Txn: From<Txn>;
    type Value: ObjectValue;
}

impl Object for Any {
    type Txn = Txn;
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

impl ObjectValue for i64 {
    fn cast_from(v: Value) -> Result<Self> {
        if let Value::I64Value(v) = v {
            Ok(v)
        } else {
            Err(Error::invalid_argument(format!("{:?} to i64", v)))
        }
    }
}

impl ObjectValue for Vec<u8> {
    fn cast_from(v: Value) -> Result<Self> {
        if let Value::BlobValue(v) = v {
            Ok(v)
        } else {
            Err(Error::invalid_argument(format!("{:?} to Vec<u8>", v)))
        }
    }
}

impl ObjectValue for String {
    fn cast_from(v: Value) -> Result<Self> {
        if let Value::TextValue(v) = v {
            Ok(v)
        } else {
            Err(Error::invalid_argument(format!("{:?} to String", v,)))
        }
    }
}

impl<K, V> ObjectValue for HashMap<K, V>
where
    K: ObjectValue + Ord + Hash,
    V: ObjectValue,
{
    fn cast_from(v: Value) -> Result<Self> {
        if let Value::MappingValue(v) = v {
            let keys: Result<Vec<K>> = v
                .keys
                .into_iter()
                .map(|x| {
                    x.value
                        .ok_or_else(|| Error::invalid_argument("missing value"))
                        .and_then(K::cast_from)
                })
                .collect();
            let values: Result<Vec<V>> = v
                .values
                .into_iter()
                .map(|x| {
                    x.value
                        .ok_or_else(|| Error::invalid_argument("missing value"))
                        .and_then(V::cast_from)
                })
                .collect();
            Ok(keys?.into_iter().zip(values?).collect())
        } else {
            Err(Error::invalid_argument(format!("{:?} to Map", v,)))
        }
    }
}

impl<T: ObjectValue> ObjectValue for Vec<T> {
    fn cast_from(v: Value) -> Result<Self> {
        if let Value::RepeatedValue(v) = v {
            v.values
                .into_iter()
                .map(|x| {
                    x.value
                        .ok_or_else(|| Error::invalid_argument("missing value"))
                        .and_then(T::cast_from)
                })
                .collect()
        } else {
            Err(Error::invalid_argument(format!("{:?} to Vec", v,)))
        }
    }
}
