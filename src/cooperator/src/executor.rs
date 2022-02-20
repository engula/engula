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

use std::collections::{BTreeMap, VecDeque};

use engula_apis::*;

use crate::{Error, Result};

pub struct Executor {
    read_cache: BTreeMap<Vec<u8>, Value>,
    _write_cache: BTreeMap<Vec<u8>, Vec<Expr>>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            read_cache: BTreeMap::new(),
            _write_cache: BTreeMap::new(),
        }
    }

    pub fn execute(&mut self, expr: Expr) -> Result<ExprResult> {
        let id = if let Some(expr::From::Id(id)) = expr.from {
            id
        } else {
            return Err(Error::InvalidArgument);
        };
        let mut result = ExprResult::default();
        if let Some(call) = expr.call {
            self.handle_object_call(&id, call, &mut result)?;
        } else {
            for expr in expr.subexprs {
                let mut res = self.handle_object_expr(&id, expr)?;
                let value = if res.values.len() <= 1 {
                    res.values.pop().unwrap_or_default()
                } else {
                    let value = RepeatedValue { values: res.values };
                    ValueUnion {
                        value: Some(Value::RepeatedValue(value)),
                    }
                };
                result.values.push(value);
            }
        }
        Ok(result)
    }

    fn handle_object_expr(&mut self, id: &[u8], expr: Expr) -> Result<ExprResult> {
        let mut result = ExprResult::default();
        if let Some(call) = expr.call {
            self.handle_object_call(id, call, &mut result)?;
        } else if let Some(expr::From::Index(at)) = expr.from {
            for expr in expr.subexprs {
                let call = expr.call.ok_or(Error::InvalidArgument)?;
                self.handle_member_call(id, &at, call, &mut result)?;
            }
        } else {
            return Err(Error::InvalidArgument);
        }
        Ok(result)
    }

    fn handle_object_call(
        &mut self,
        id: &[u8],
        call: CallExpr,
        result: &mut ExprResult,
    ) -> Result<()> {
        let func = Function::from_i32(call.func).ok_or(Error::InvalidArgument)?;
        let mut args = Args::new(call.args);
        match func {
            Function::Nop => {}
            Function::Load => {
                let value = self.read_cache.get(id).cloned();
                result.values.push(value.into());
            }
            Function::Store => {
                let value = args.take()?;
                self.read_cache.insert(id.to_owned(), value);
            }
            Function::Reset => {
                self.read_cache.remove(id);
            }
            Function::Add | Function::Sub => {
                if let Some(value) = self.read_cache.get_mut(id) {
                    if let Value::I64Value(v) = value {
                        let operand = args.take_i64()?;
                        if func == Function::Add {
                            *v += operand;
                        } else {
                            *v -= operand;
                        }
                    } else {
                        return Err(Error::InvalidArgument);
                    }
                } else {
                    let value = args.take_numeric()?;
                    self.read_cache.insert(id.to_owned(), value);
                }
            }
            Function::Len => {
                let len = if let Some(value) = self.read_cache.get(id) {
                    match value {
                        Value::BlobValue(v) => v.len(),
                        Value::TextValue(v) => v.len(),
                        Value::MappingValue(v) => v.keys.len(),
                        Value::RepeatedValue(v) => v.values.len(),
                        _ => return Err(Error::InvalidArgument),
                    }
                } else {
                    0
                };
                result.values.push(Value::I64Value(len as i64).into());
            }
            Function::Append => {
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::BlobValue(v) => {
                            let mut operand = args.take_blob()?;
                            v.append(&mut operand);
                        }
                        Value::TextValue(v) => {
                            let operand = args.take_text()?;
                            v.push_str(&operand);
                        }
                        Value::RepeatedValue(v) => {
                            let mut operand = args.take_repeated()?;
                            v.values.append(&mut operand.values);
                        }
                        _ => return Err(Error::InvalidArgument),
                    }
                } else {
                    let value = args.take_sequence()?;
                    self.read_cache.insert(id.to_owned(), value);
                }
            }
            Function::PushBack => {
                let operand = args.take()?;
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::RepeatedValue(v) => {
                            v.values.push(operand.into());
                        }
                        _ => return Err(Error::InvalidArgument),
                    }
                } else {
                    let value = RepeatedValue {
                        values: vec![operand.into()],
                    };
                    self.read_cache.insert(id.to_owned(), value.into());
                }
            }
            Function::PushFront => {
                let operand = args.take()?;
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::RepeatedValue(v) => {
                            v.values.insert(0, operand.into());
                        }
                        _ => return Err(Error::InvalidArgument),
                    }
                } else {
                    let value = RepeatedValue {
                        values: vec![operand.into()],
                    };
                    self.read_cache.insert(id.to_owned(), value.into());
                }
            }
        }
        Ok(())
    }

    fn handle_member_call(
        &mut self,
        id: &[u8],
        at: &ValueUnion,
        call: CallExpr,
        result: &mut ExprResult,
    ) -> Result<()> {
        let func = Function::from_i32(call.func).ok_or(Error::InvalidArgument)?;
        let mut args = Args::new(call.args);
        match func {
            Function::Nop => {}
            Function::Load => {
                if let Some(value) = self.read_cache.get(id) {
                    match value {
                        Value::MappingValue(v) => {
                            if let Some(index) = v.keys.iter().position(|x| x == at) {
                                result.values.push(v.values[index].clone());
                            }
                        }
                        Value::RepeatedValue(v) => {
                            if let Some(Value::I64Value(index)) = at.value {
                                let index = index as usize;
                                if index < v.values.len() {
                                    result.values.push(v.values[index].clone());
                                } else {
                                    return Err(Error::InvalidArgument);
                                }
                            }
                        }
                        _ => return Err(Error::InvalidArgument),
                    }
                }
            }
            Function::Store => {
                let operand = args.take()?;
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::MappingValue(v) => {
                            if let Some(index) = v.keys.iter().position(|x| x == at) {
                                v.values[index] = operand.into();
                            } else {
                                v.keys.push(at.to_owned());
                                v.values.push(operand.into());
                            }
                        }
                        Value::RepeatedValue(v) => {
                            if let Some(Value::I64Value(index)) = at.value {
                                let index = index as usize;
                                if index < v.values.len() {
                                    v.values[index] = operand.into();
                                } else {
                                    return Err(Error::InvalidArgument);
                                }
                            }
                        }
                        _ => return Err(Error::InvalidArgument),
                    }
                } else {
                    match at.value {
                        Some(Value::BlobValue(_)) => {
                            let value = MappingValue {
                                keys: vec![at.to_owned()],
                                values: vec![operand.into()],
                            };
                            self.read_cache.insert(id.to_owned(), value.into());
                        }
                        _ => return Err(Error::InvalidArgument),
                    }
                }
            }
            Function::Reset => {
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::MappingValue(v) => {
                            if let Some(index) = v.keys.iter().position(|x| x == at) {
                                v.keys.remove(index);
                                v.values.remove(index);
                            }
                        }
                        Value::RepeatedValue(v) => {
                            if let Some(Value::I64Value(index)) = at.value {
                                let index = index as usize;
                                if index < v.values.len() {
                                    v.values.remove(index);
                                } else {
                                    return Err(Error::InvalidArgument);
                                }
                            }
                        }
                        _ => return Err(Error::InvalidArgument),
                    }
                }
            }
            _ => return Err(Error::InvalidArgument),
        }
        Ok(())
    }
}

struct Args {
    args: VecDeque<ValueUnion>,
}

impl Args {
    fn new(args: Vec<ValueUnion>) -> Self {
        Self { args: args.into() }
    }

    fn take(&mut self) -> Result<Value> {
        self.args
            .pop_front()
            .and_then(|v| v.value)
            .ok_or(Error::InvalidArgument)
    }

    fn take_i64(&mut self) -> Result<i64> {
        match self.take()? {
            Value::I64Value(v) => Ok(v),
            _ => Err(Error::InvalidArgument),
        }
    }

    fn take_numeric(&mut self) -> Result<Value> {
        let v = self.take()?;
        match v {
            Value::I64Value(_) => Ok(v),
            _ => Err(Error::InvalidArgument),
        }
    }

    fn take_blob(&mut self) -> Result<Vec<u8>> {
        match self.take()? {
            Value::BlobValue(v) => Ok(v),
            _ => Err(Error::InvalidArgument),
        }
    }

    fn take_text(&mut self) -> Result<String> {
        match self.take()? {
            Value::TextValue(v) => Ok(v),
            _ => Err(Error::InvalidArgument),
        }
    }

    fn take_repeated(&mut self) -> Result<RepeatedValue> {
        match self.take()? {
            Value::RepeatedValue(v) => Ok(v),
            _ => Err(Error::InvalidArgument),
        }
    }

    fn take_sequence(&mut self) -> Result<Value> {
        let v = self.take()?;
        match v {
            Value::BlobValue(_) => Ok(v),
            Value::TextValue(_) => Ok(v),
            Value::RepeatedValue(_) => Ok(v),
            _ => Err(Error::InvalidArgument),
        }
    }
}
