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

use std::{collections::HashMap, sync::Arc};

use engula_apis::v1::*;
use tokio::sync::Mutex;

use super::{Args, Write, WriteBatch};
use crate::{Error, Result};

#[derive(Clone)]
pub struct Collection {
    objects: Arc<Mutex<HashMap<Vec<u8>, TypedValue>>>,
}

impl Collection {
    pub fn new(_: CollectionDesc) -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get<T: TryFrom<TypedValue>>(&self, id: &[u8]) -> Result<T> {
        let objects = self.objects.lock().await;
        let ob = objects.get(id).cloned().unwrap_or_default();
        ob.try_into()
            .map_err(|_| Error::invalid_argument("object type mismatch"))
    }

    pub async fn write(&self, wb: WriteBatch) {
        let mut objects = self.objects.lock().await;
        for write in wb.writes {
            match write {
                Write::Put(id, value) => {
                    objects.insert(id, value);
                }
                Write::Delete(id) => {
                    objects.remove(&id);
                }
            }
        }
    }

    pub async fn execute(
        &self,
        wb: &mut WriteBatch,
        req: CollectionRequest,
    ) -> Result<CollectionResponse> {
        if req.ids.len() != req.exprs.len() {
            return Err(Error::invalid_argument("ids and exprs don't match"));
        }
        let mut res = CollectionResponse::default();
        for (id, expr) in req.ids.into_iter().zip(req.exprs.into_iter()) {
            let value = self.execute_expr(wb, id, expr).await?;
            res.values.push(value);
        }
        Ok(res)
    }

    async fn execute_expr(
        &self,
        wb: &mut WriteBatch,
        id: Vec<u8>,
        expr: TypedExpr,
    ) -> Result<TypedValue> {
        let expr = expr
            .expr
            .ok_or_else(|| Error::invalid_argument("missing expression"))?;
        match expr {
            Expr::AnyExpr(expr) => self.execute_any_expr(wb, id, expr).await,
            Expr::I64Expr(expr) => self.execute_i64_expr(wb, id, expr).await,
            Expr::BlobExpr(expr) => self.execute_blob_expr(wb, id, expr).await,
            _ => todo!(),
        }
    }

    async fn execute_any_expr(
        &self,
        wb: &mut WriteBatch,
        id: Vec<u8>,
        expr: AnyExpr,
    ) -> Result<TypedValue> {
        let (func, mut args) = parse_call_expr(expr.call)?;
        match func {
            Function::Get => {
                let ob: TypedValue = self.get(&id).await?;
                Ok(ob)
            }
            Function::Set => {
                let value: TypedValue = args.take()?;
                wb.put(id, value);
                Ok(TypedValue::default())
            }
            Function::Delete => {
                wb.delete(id);
                Ok(TypedValue::default())
            }
            Function::Exists => {
                let ob: TypedValue = self.get(&id).await?;
                Ok(ob.value.is_some().into())
            }
            _ => Err(Error::invalid_argument("unsupported any function")),
        }
    }

    async fn execute_i64_expr(
        &self,
        wb: &mut WriteBatch,
        id: Vec<u8>,
        expr: I64Expr,
    ) -> Result<TypedValue> {
        let (func, mut args) = parse_call_expr(expr.call)?;
        match func {
            func @ (Function::Add | Function::Sub) => {
                let ob: Option<i64> = self.get(&id).await?;
                let value = ob.unwrap_or(0);
                let operand: i64 = args.take()?;
                let new_value = match func {
                    Function::Add => value + operand,
                    Function::Sub => value - operand,
                    _ => unreachable!(),
                };
                wb.put(id, new_value.into());
                Ok(TypedValue::default())
            }
            _ => Err(Error::invalid_argument("unsupported i64 function")),
        }
    }

    async fn execute_blob_expr(
        &self,
        _: &mut WriteBatch,
        id: Vec<u8>,
        expr: BlobExpr,
    ) -> Result<TypedValue> {
        let (func, _) = parse_call_expr(expr.call)?;
        let object: Option<Vec<u8>> = self.get(&id).await?;
        match func {
            Function::Len => Ok(object.map(|v| v.len() as i64).into()),
            _ => Err(Error::invalid_argument("unsupported blob function")),
        }
    }
}

fn parse_call_expr(call: Option<CallExpr>) -> Result<(Function, Args)> {
    let call = call.ok_or_else(|| Error::invalid_argument("missing call"))?;
    let func =
        Function::from_i32(call.func).ok_or_else(|| Error::invalid_argument("unknown function"))?;
    let args = Args::new(call.args);
    Ok((func, args))
}
