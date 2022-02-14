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

use std::collections::{HashMap, VecDeque};

use engula_apis::*;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::{Error, Result};

type TonicResult<T> = std::result::Result<T, Status>;

pub struct Transactor {
    uv: Mutex<Universe>,
}

impl Transactor {
    fn new() -> Self {
        Self {
            uv: Mutex::new(Universe::new()),
        }
    }

    pub fn new_service() -> txn_server::TxnServer<Self> {
        txn_server::TxnServer::new(Self::new())
    }
}

#[tonic::async_trait]
impl txn_server::Txn for Transactor {
    async fn batch(
        &self,
        req: Request<BatchTxnRequest>,
    ) -> TonicResult<Response<BatchTxnResponse>> {
        let req = req.into_inner();
        let res = self.uv.lock().await.execute(req).await?;
        Ok(Response::new(res))
    }
}

struct Universe {
    databases: HashMap<String, Database>,
}

impl Universe {
    fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }

    async fn execute(&mut self, req: BatchTxnRequest) -> Result<BatchTxnResponse> {
        let mut res = BatchTxnResponse::default();
        for sub_req in req.requests {
            // Assumes that all databases exist for now.
            let db = self
                .databases
                .entry(sub_req.name.clone())
                .or_insert_with(Database::new);
            let sub_res = db.execute(sub_req)?;
            res.responses.push(sub_res);
        }
        Ok(res)
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
            let func = call.func;
            let mut args = Args::new(call.args);
            match Function::from_i32(func).ok_or(Error::InvalidRequest)? {
                Function::Get => {
                    result.value = self.objects.get(&id).cloned();
                }
                Function::Set => {
                    let value = args.take()?;
                    self.objects.insert(id, value);
                }
                Function::Delete => {
                    self.objects.remove(&id);
                }
                func => {
                    if let Some(v) = self.objects.get_mut(&id).and_then(|v| v.value.as_mut()) {
                        match v {
                            value::Value::BlobValue(v) => {
                                return Self::handle_blob_call(v, func, args);
                            }
                            value::Value::Int64Value(v) => {
                                return Self::handle_int64_call(v, func, args);
                            }
                            value::Value::SequenceValue(v) => {
                                return Self::handle_sequence_call(v, func, args);
                            }
                            value::Value::AssociativeValue(v) => {
                                return Self::handle_associative_call(v, func, args);
                            }
                            _ => return Err(Error::InvalidRequest),
                        }
                    } else {
                        return self.handle_none_call(id, func, args);
                    }
                }
            }
        } else if let Some(v) = self.objects.get_mut(&id).and_then(|v| v.value.as_mut()) {
            match v {
                value::Value::SequenceValue(v) => {
                    return Self::handle_sequence_subcalls(v, expr.subcalls);
                }
                value::Value::AssociativeValue(v) => {
                    return Self::handle_associative_subcalls(v, expr.subcalls);
                }
                _ => return Err(Error::InvalidRequest),
            }
        } else {
            return self.handle_none_subcalls(id, expr.subcalls);
        }
        Ok(result)
    }

    fn handle_none_call(
        &mut self,
        id: Vec<u8>,
        func: Function,
        mut args: Args,
    ) -> Result<ExprResult> {
        let result = ExprResult::default();
        match func {
            Function::Get => {}
            Function::Set => {
                let value = args.take()?;
                self.objects.insert(id, value);
            }
            Function::AddAssign => {
                let value = args.take_numeric()?;
                self.objects.insert(id, value);
            }
            Function::SubAssign => {
                let value = args.take_numeric()?;
                self.objects.insert(id, value);
            }
            _ => return Err(Error::InvalidRequest),
        }
        Ok(result)
    }

    fn handle_none_subcalls(
        &mut self,
        _id: Vec<u8>,
        subcalls: Vec<CallExpr>,
    ) -> Result<ExprResult> {
        let result = ExprResult::default();
        for call in subcalls {
            let func = call.func;
            match Function::from_i32(func).ok_or(Error::InvalidRequest)? {
                Function::Get => {}
                _ => return Err(Error::InvalidRequest),
            }
        }
        Ok(result)
    }

    fn handle_blob_call(ob: &mut Vec<u8>, func: Function, mut args: Args) -> Result<ExprResult> {
        let mut result = ExprResult::default();
        match func {
            Function::Len => {
                result.value = Some(Value {
                    value: Some(value::Value::Int64Value(ob.len() as i64)),
                });
            }
            Function::Append => {
                let mut value = args.take_blob()?;
                ob.append(&mut value);
            }
            _ => return Err(Error::InvalidRequest),
        }
        Ok(result)
    }

    fn handle_int64_call(ob: &mut i64, func: Function, mut args: Args) -> Result<ExprResult> {
        let result = ExprResult::default();
        match func {
            Function::AddAssign => {
                *ob += args.take_i64()?;
            }
            Function::SubAssign => {
                *ob -= args.take_i64()?;
            }
            _ => return Err(Error::InvalidRequest),
        }
        Ok(result)
    }

    fn handle_sequence_call(
        ob: &mut SequenceValue,
        func: Function,
        mut args: Args,
    ) -> Result<ExprResult> {
        let mut result = ExprResult::default();
        match func {
            Function::Len => {
                result.value = Some(Value {
                    value: Some(value::Value::Int64Value(ob.values.len() as i64)),
                });
            }
            Function::Pop => {
                result.value = ob.values.pop();
            }
            Function::Push => {
                ob.values.push(args.take()?);
            }
            _ => return Err(Error::InvalidRequest),
        }
        Ok(result)
    }

    fn handle_sequence_subcalls(
        ob: &mut SequenceValue,
        subcalls: Vec<CallExpr>,
    ) -> Result<ExprResult> {
        let mut result = ExprResult::default();
        for call in subcalls {
            let func = call.func;
            let mut args = Args::new(call.args);
            let index = args.take_i64()? as usize;
            match Function::from_i32(func).ok_or(Error::InvalidRequest)? {
                Function::Get => {
                    result.value = ob.values.get(index).cloned();
                }
                Function::Set => {
                    if ob.values.len() <= index {
                        return Err(Error::InvalidRequest);
                    }
                    ob.values[index] = args.take()?;
                }
                _ => return Err(Error::InvalidRequest),
            }
        }
        Ok(result)
    }

    fn handle_associative_call(
        ob: &mut AssociativeValue,
        func: Function,
        _args: Args,
    ) -> Result<ExprResult> {
        let mut result = ExprResult::default();
        match func {
            Function::Len => {
                result.value = Some(Value {
                    value: Some(value::Value::Int64Value(ob.keys.len() as i64)),
                });
            }
            _ => return Err(Error::InvalidRequest),
        }
        Ok(result)
    }

    fn handle_associative_subcalls(
        ob: &mut AssociativeValue,
        subcalls: Vec<CallExpr>,
    ) -> Result<ExprResult> {
        let mut result = ExprResult::default();
        for call in subcalls {
            let func = call.func;
            let mut args = Args::new(call.args);
            let key = args.take_blob()?;
            let index = ob.keys.iter().position(|k| k == &key);
            match Function::from_i32(func).ok_or(Error::InvalidRequest)? {
                Function::Get => {
                    result.value = index.map(|i| ob.values[i].clone());
                }
                Function::Set => {
                    let value = args.take()?;
                    if let Some(i) = index {
                        ob.values[i] = value;
                    } else {
                        ob.keys.push(key);
                        ob.values.push(value);
                    }
                }
                Function::Delete => {
                    if let Some(i) = index {
                        ob.keys.swap_remove(i);
                        ob.values.swap_remove(i);
                    }
                }
                _ => return Err(Error::InvalidRequest),
            }
        }
        Ok(result)
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
}
