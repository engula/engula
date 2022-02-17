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

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use engula_apis::*;
use tokio::sync::Mutex;

use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Transactor {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    databases: HashMap<String, Database>,
}

impl Transactor {
    pub fn new() -> Self {
        let inner = Inner {
            databases: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn execute(&self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let mut inner = self.inner.lock().await;
        // Assumes that all databases exist for now.
        let db = inner
            .databases
            .entry(req.name.clone())
            .or_insert_with(Database::new);
        db.execute(req)
    }
}

struct Database {
    collections: HashMap<String, Collection>,
}

impl Database {
    fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    fn execute(&mut self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let mut res = DatabaseTxnResponse::default();
        for co_req in req.requests {
            // Assumes that all collections exist for now.
            let co = self
                .collections
                .entry(co_req.name.clone())
                .or_insert_with(Collection::new);
            let co_res = co.execute(co_req)?;
            res.responses.push(co_res);
        }
        Ok(res)
    }
}

struct Collection {
    objects: HashMap<Vec<u8>, Value>,
}

impl Collection {
    fn new() -> Self {
        Self {
            objects: HashMap::new(),
        }
    }

    fn execute(&mut self, req: CollectionTxnRequest) -> Result<CollectionTxnResponse> {
        let mut res = CollectionTxnResponse::default();
        for expr in req.exprs {
            let result = self.execute_expr(expr)?;
            res.results.push(result);
        }
        Ok(res)
    }

    fn execute_expr(&mut self, expr: Expr) -> Result<ExprResult> {
        let id = expr.id;
        let mut result = ExprResult::default();
        if let Some(call) = expr.call {
            let func = Function::from_i32(call.func).ok_or(Error::InvalidRequest)?;
            let mut args = Args::new(call.args);
            match func {
                Function::Get => {
                    result.value = self.objects.get(&id).cloned();
                }
                Function::Set => {
                    let value = args.take()?;
                    self.objects.insert(id, value);
                }
                Function::Remove => {
                    self.objects.remove(&id);
                }
                _ => return Err(Error::InvalidRequest),
            }
        } else {
            for call in expr.subcalls {
                if let Some(object) = self.objects.get_mut(&id).and_then(|v| v.value.as_mut()) {
                    match object {
                        value::Value::BlobValue(object) => {
                            handle_blob_call(call, object, &mut result)?;
                        }
                        value::Value::Int64Value(object) => {
                            handle_int64_call(call, object, &mut result)?;
                        }
                        value::Value::SequenceValue(object) => {
                            handle_sequence_call(call, object, &mut result)?;
                        }
                        value::Value::AssociativeValue(object) => {
                            handle_associative_call(call, object, &mut result)?;
                        }
                        _ => return Err(Error::InvalidRequest),
                    }
                } else {
                    self.handle_none_call(&id, call, &mut result)?;
                }
            }
        }

        Ok(result)
    }

    fn handle_none_call(
        &mut self,
        id: &[u8],
        call: CallExpr,
        result: &mut ExprResult,
    ) -> Result<()> {
        let func = Function::from_i32(call.func).ok_or(Error::InvalidRequest)?;
        let mut args = Args::new(call.args);
        match func {
            Function::Add => {
                let value = args.take_numeric()?;
                self.objects.insert(id.to_owned(), value);
            }
            Function::Sub => {
                let value = args.take_numeric()?;
                self.objects.insert(id.to_owned(), value);
            }
            Function::Len => {
                result.value = Some(Value {
                    value: Some(value::Value::Int64Value(0)),
                });
            }
            Function::Append => {
                let value = args.take_sequence()?;
                self.objects.insert(id.to_owned(), value);
            }
            Function::PopBack | Function::PopFront => {}
            Function::PushBack | Function::PushFront => {
                let value = SequenceValue {
                    values: vec![args.take()?],
                };
                let value = Value {
                    value: Some(value::Value::SequenceValue(value)),
                };
                self.objects.insert(id.to_owned(), value);
            }
            Function::Get => {}
            Function::Set => {
                let value = AssociativeValue {
                    keys: vec![args.take_blob()?],
                    values: vec![args.take()?],
                };
                let value = Value {
                    value: Some(value::Value::AssociativeValue(value)),
                };
                self.objects.insert(id.to_owned(), value);
            }
            Function::Remove => {}
            _ => return Err(Error::InvalidRequest),
        }
        Ok(())
    }
}

struct Args {
    args: VecDeque<Value>,
}

impl Args {
    fn new(args: Vec<Value>) -> Self {
        Self { args: args.into() }
    }

    fn take(&mut self) -> Result<Value> {
        self.args.pop_front().ok_or(Error::InvalidRequest)
    }

    fn take_blob(&mut self) -> Result<Vec<u8>> {
        match self.take()?.value {
            Some(value::Value::BlobValue(v)) => Ok(v),
            _ => Err(Error::InvalidRequest),
        }
    }

    fn take_i64(&mut self) -> Result<i64> {
        match self.take()?.value {
            Some(value::Value::Int64Value(v)) => Ok(v),
            _ => Err(Error::InvalidRequest),
        }
    }

    fn take_numeric(&mut self) -> Result<Value> {
        let v = self.take()?;
        match v.value.as_ref() {
            Some(value::Value::Int64Value(_)) => Ok(v),
            _ => Err(Error::InvalidRequest),
        }
    }

    fn take_sequence(&mut self) -> Result<Value> {
        let v = self.take()?;
        match v.value.as_ref() {
            Some(value::Value::BlobValue(_)) => Ok(v),
            Some(value::Value::TextValue(_)) => Ok(v),
            Some(value::Value::SequenceValue(_)) => Ok(v),
            _ => Err(Error::InvalidRequest),
        }
    }
}

fn handle_blob_call(call: CallExpr, object: &mut Vec<u8>, result: &mut ExprResult) -> Result<()> {
    let func = Function::from_i32(call.func).ok_or(Error::InvalidRequest)?;
    let mut args = Args::new(call.args);
    match func {
        Function::Len => {
            result.value = Some(Value {
                value: Some(value::Value::Int64Value(object.len() as i64)),
            });
        }
        Function::Append => {
            let mut value = args.take_blob()?;
            object.append(&mut value);
        }
        _ => return Err(Error::InvalidRequest),
    }
    Ok(())
}

fn handle_int64_call(call: CallExpr, object: &mut i64, _: &mut ExprResult) -> Result<()> {
    let func = Function::from_i32(call.func).ok_or(Error::InvalidRequest)?;
    let mut args = Args::new(call.args);
    match func {
        Function::Add => {
            *object += args.take_i64()?;
        }
        Function::Sub => {
            *object -= args.take_i64()?;
        }
        _ => return Err(Error::InvalidRequest),
    }
    Ok(())
}

fn handle_sequence_call(
    call: CallExpr,
    object: &mut SequenceValue,
    result: &mut ExprResult,
) -> Result<()> {
    let func = Function::from_i32(call.func).ok_or(Error::InvalidRequest)?;
    let mut args = Args::new(call.args);
    match func {
        Function::Len => {
            result.value = Some(Value {
                value: Some(value::Value::Int64Value(object.values.len() as i64)),
            });
        }
        Function::Get => {
            let index = args.take_i64()? as usize;
            result.value = object.values.get(index).cloned();
        }
        Function::Set => {
            let index = args.take_i64()? as usize;
            if object.values.len() <= index {
                return Err(Error::InvalidRequest);
            }
            object.values[index] = args.take()?;
        }
        Function::PopBack => {
            result.value = object.values.pop();
        }
        Function::PopFront => {
            result.value = if !object.values.is_empty() {
                Some(object.values.remove(0))
            } else {
                None
            }
        }
        Function::PushBack => {
            object.values.push(args.take()?);
        }
        Function::PushFront => {
            object.values.insert(0, args.take()?);
        }
        _ => return Err(Error::InvalidRequest),
    }
    Ok(())
}

fn handle_associative_call(
    call: CallExpr,
    object: &mut AssociativeValue,
    result: &mut ExprResult,
) -> Result<()> {
    let func = Function::from_i32(call.func).ok_or(Error::InvalidRequest)?;
    let mut args = Args::new(call.args);
    match func {
        Function::Len => {
            result.value = Some(Value {
                value: Some(value::Value::Int64Value(object.keys.len() as i64)),
            });
        }
        Function::Get => {
            let key = args.take_blob()?;
            let index = object.keys.iter().position(|k| k == &key);
            result.value = index.map(|i| object.values[i].clone());
        }
        Function::Set => {
            let key = args.take_blob()?;
            let index = object.keys.iter().position(|k| k == &key);
            let value = args.take()?;
            if let Some(i) = index {
                object.values[i] = value;
            } else {
                object.keys.push(key);
                object.values.push(value);
            }
        }
        Function::Remove => {
            let key = args.take_blob()?;
            let index = object.keys.iter().position(|k| k == &key);
            if let Some(i) = index {
                object.keys.swap_remove(i);
                object.values.swap_remove(i);
            }
        }
        _ => return Err(Error::InvalidRequest),
    }
    Ok(())
}
