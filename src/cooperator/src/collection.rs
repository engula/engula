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

use std::{collections::BTreeMap, sync::Arc};

use engula_apis::*;
use tokio::sync::Mutex;

use crate::{Args, Error, Result};

#[derive(Clone)]
pub struct Collection {
    inner: Arc<Mutex<Inner>>,
}

impl Collection {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    pub async fn execute(&self, req: CollectionTxnRequest) -> Result<CollectionTxnResponse> {
        let mut inner = self.inner.lock().await;
        let mut res = CollectionTxnResponse::default();
        for expr in req.exprs {
            let result = inner.handle_expr(expr)?;
            res.results.push(result);
        }
        Ok(res)
    }
}

struct Inner {
    read_cache: BTreeMap<Vec<u8>, Value>,
    _write_cache: BTreeMap<Vec<u8>, Vec<Expr>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            read_cache: BTreeMap::new(),
            _write_cache: BTreeMap::new(),
        }
    }

    fn handle_expr(&mut self, expr: Expr) -> Result<ExprResult> {
        let id = if let Some(expr::From::Id(id)) = expr.from {
            id
        } else {
            return Err(Error::invalid_argument("missing object id"));
        };
        let mut result = ExprResult::default();
        if let Some(call) = expr.call {
            self.handle_object_call(&id, call, &mut result)?;
        } else {
            let mut res = self.handle_object_exprs(&id, expr.subexprs)?;
            let value = if res.values.len() <= 1 {
                res.values.pop().unwrap_or_default()
            } else {
                ListValue { values: res.values }.into()
            };
            result.values.push(value);
        }
        Ok(result)
    }

    fn handle_object_exprs(&mut self, id: &[u8], exprs: Vec<Expr>) -> Result<ExprResult> {
        let mut result = ExprResult::default();
        for expr in exprs {
            let call = expr
                .call
                .ok_or_else(|| Error::invalid_argument("missing call expr"))?;
            if let Some(expr::From::Index(index)) = expr.from {
                self.handle_member_call(id, call, index, &mut result)?;
            } else {
                self.handle_object_call(id, call, &mut result)?;
            }
        }
        Ok(result)
    }

    fn handle_object_call(
        &mut self,
        id: &[u8],
        call: CallExpr,
        result: &mut ExprResult,
    ) -> Result<()> {
        let func = Function::from_i32(call.func)
            .ok_or_else(|| Error::invalid_argument("invalid function"))?;
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
                        return Err(Error::invalid_argument("require numeric object"));
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
                        Value::MapValue(v) => v.keys.len(),
                        Value::ListValue(v) => v.values.len(),
                        _ => return Err(Error::invalid_argument("require container object")),
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
                        Value::ListValue(v) => {
                            let mut operand = args.take_list()?;
                            v.values.append(&mut operand.values);
                        }
                        _ => return Err(Error::invalid_argument("require sequence object")),
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
                        Value::ListValue(v) => {
                            v.values.push(operand.into());
                        }
                        _ => return Err(Error::invalid_argument("require sequence object")),
                    }
                } else {
                    let value = ListValue {
                        values: vec![operand.into()],
                    };
                    self.read_cache.insert(id.to_owned(), value.into());
                }
            }
            Function::PushFront => {
                let operand = args.take()?;
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::ListValue(v) => {
                            v.values.insert(0, operand.into());
                        }
                        _ => return Err(Error::invalid_argument("require sequence object")),
                    }
                } else {
                    let value = ListValue {
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
        call: CallExpr,
        index: ValueUnion,
        result: &mut ExprResult,
    ) -> Result<()> {
        let func = Function::from_i32(call.func)
            .ok_or_else(|| Error::invalid_argument("invalid function"))?;
        let mut args = Args::new(call.args);
        match func {
            Function::Nop => {}
            Function::Load => {
                if let Some(value) = self.read_cache.get(id) {
                    match value {
                        Value::MapValue(v) => {
                            if let Some(pos) = v.keys.iter().position(|x| x == &index) {
                                result.values.push(v.values[pos].clone());
                            }
                        }
                        Value::ListValue(v) => {
                            if let Some(Value::I64Value(mut pos)) = index.value {
                                // TODO: do more checks here
                                let len = v.values.len() as i64;
                                pos += len;
                                if pos >= 0 && pos < len {
                                    result.values.push(v.values[pos as usize].clone());
                                } else {
                                    return Err(Error::invalid_argument("index out of range"));
                                }
                            }
                        }
                        _ => return Err(Error::invalid_argument("require container object")),
                    }
                }
            }
            Function::Store => {
                let operand = args.take()?;
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::MapValue(v) => {
                            if let Some(pos) = v.keys.iter().position(|x| x == &index) {
                                v.values[pos] = operand.into();
                            } else {
                                v.keys.push(index);
                                v.values.push(operand.into());
                            }
                        }
                        Value::ListValue(v) => {
                            if let Some(Value::I64Value(mut pos)) = index.value {
                                let len = v.values.len() as i64;
                                pos += len;
                                if pos >= 0 && pos < len {
                                    v.values[pos as usize] = operand.into();
                                } else {
                                    return Err(Error::invalid_argument("index out of range"));
                                }
                            }
                        }
                        _ => return Err(Error::invalid_argument("require container object")),
                    }
                } else {
                    match index.value {
                        Some(Value::BlobValue(_)) => {
                            let value = MapValue {
                                keys: vec![index],
                                values: vec![operand.into()],
                            };
                            self.read_cache.insert(id.to_owned(), value.into());
                        }
                        _ => return Err(Error::invalid_argument("require blob index")),
                    }
                }
            }
            Function::Reset => {
                if let Some(value) = self.read_cache.get_mut(id) {
                    match value {
                        Value::MapValue(v) => {
                            if let Some(pos) = v.keys.iter().position(|x| x == &index) {
                                v.keys.remove(pos);
                                v.values.remove(pos);
                            }
                        }
                        Value::ListValue(v) => {
                            if let Some(Value::I64Value(mut pos)) = index.value {
                                let len = v.values.len() as i64;
                                pos += len;
                                if pos >= 0 && pos < len {
                                    v.values.remove(pos as usize);
                                } else {
                                    return Err(Error::invalid_argument("index out of range"));
                                }
                            }
                        }
                        _ => return Err(Error::invalid_argument("require container object")),
                    }
                }
            }
            _ => return Err(Error::invalid_argument("invalid member function")),
        }
        Ok(())
    }
}
