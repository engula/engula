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

use std::collections::VecDeque;

use engula_apis::*;

use crate::{Error, Result};

pub struct Args(VecDeque<GenericValue>);

impl Args {
    pub fn new(args: Vec<GenericValue>) -> Self {
        Self(args.into())
    }

    pub fn take(&mut self) -> Result<Value> {
        self.0
            .pop_front()
            .and_then(|v| v.value)
            .ok_or_else(|| Error::invalid_argument("missing argument"))
    }

    pub fn take_i64(&mut self) -> Result<i64> {
        match self.take()? {
            Value::I64Value(v) => Ok(v),
            _ => Err(Error::invalid_argument("require i64")),
        }
    }

    pub fn take_numeric(&mut self) -> Result<Value> {
        let v = self.take()?;
        match v {
            Value::I64Value(_) => Ok(v),
            _ => Err(Error::invalid_argument("require numeric")),
        }
    }

    pub fn take_blob(&mut self) -> Result<Vec<u8>> {
        match self.take()? {
            Value::BlobValue(v) => Ok(v),
            _ => Err(Error::invalid_argument("require blob")),
        }
    }

    pub fn take_text(&mut self) -> Result<String> {
        match self.take()? {
            Value::TextValue(v) => Ok(v),
            _ => Err(Error::invalid_argument("require text")),
        }
    }

    pub fn take_repeated(&mut self) -> Result<RepeatedValue> {
        match self.take()? {
            Value::RepeatedValue(v) => Ok(v),
            _ => Err(Error::invalid_argument("require repeated")),
        }
    }

    pub fn take_sequence(&mut self) -> Result<Value> {
        let v = self.take()?;
        match v {
            Value::BlobValue(_) => Ok(v),
            Value::TextValue(_) => Ok(v),
            Value::RepeatedValue(_) => Ok(v),
            _ => Err(Error::invalid_argument("require sequence")),
        }
    }
}
