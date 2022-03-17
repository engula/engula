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

use std::{collections::HashMap, ops::Bound, sync::Arc};

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
            Expr::ListExpr(expr) => self.execute_list_expr(wb, id, expr).await,
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
                Ok(().into())
            }
            Function::Delete => {
                wb.delete(id);
                Ok(().into())
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
                Ok(().into())
            }
            _ => Err(Error::invalid_argument("unsupported i64 function")),
        }
    }

    async fn execute_blob_expr(
        &self,
        wb: &mut WriteBatch,
        id: Vec<u8>,
        expr: BlobExpr,
    ) -> Result<TypedValue> {
        let (func, mut args) = parse_call_expr(expr.call)?;
        let ob: Option<Vec<u8>> = self.get(&id).await?;
        match func {
            Function::Len => Ok(ob.map(|v| v.len() as i64).into()),
            Function::Range => {
                if let Some(value) = ob {
                    let range: (Bound<i64>, Bound<i64>) = args.take()?;
                    let range = adjust_range_bounds(range, value.len())?;
                    Ok(value[range].into())
                } else {
                    Ok(().into())
                }
            }
            Function::Trim => {
                if let Some(value) = ob {
                    let range: (Bound<i64>, Bound<i64>) = args.take()?;
                    let range = adjust_range_bounds(range, value.len())?;
                    wb.put(id, value[range].into());
                }
                Ok(().into())
            }
            Function::Lpop => {
                let count: i64 = args.take()?;
                if let Some(mut value) = ob {
                    let new_value = if value.len() > count as usize {
                        value.split_off(count as usize)
                    } else {
                        Vec::new()
                    };
                    wb.put(id, new_value.into());
                    Ok(value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Rpop => {
                let count: i64 = args.take()?;
                if let Some(mut value) = ob {
                    let ret_value = if value.len() > count as usize {
                        value.split_off(value.len() - count as usize)
                    } else {
                        std::mem::take(&mut value)
                    };
                    wb.put(id, value.into());
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Lpush => {
                let mut new_value: Vec<u8> = args.take()?;
                new_value.extend(ob.unwrap_or_default());
                wb.put(id, new_value.into());
                Ok(().into())
            }
            Function::Rpush => {
                let operand: Vec<u8> = args.take()?;
                let mut new_value = ob.unwrap_or_default();
                new_value.extend(operand);
                wb.put(id, new_value.into());
                Ok(().into())
            }
            _ => Err(Error::invalid_argument("unsupported blob function")),
        }
    }

    async fn execute_list_expr(
        &self,
        wb: &mut WriteBatch,
        id: Vec<u8>,
        expr: ListExpr,
    ) -> Result<TypedValue> {
        let (func, mut args) = parse_call_expr(expr.call)?;
        let ob: Option<ListValue> = self.get(&id).await?;
        match func {
            Function::Len => {
                if let Some(value) = ob {
                    let len = if !value.i64_value.is_empty() {
                        value.i64_value.len()
                    } else if !value.f64_value.is_empty() {
                        value.f64_value.len()
                    } else if !value.blob_value.is_empty() {
                        value.blob_value.len()
                    } else if !value.text_value.is_empty() {
                        value.text_value.len()
                    } else {
                        0
                    };
                    let len =
                        i64::try_from(len).map_err(|_| Error::internal("convert usize to i64"))?;
                    Ok(len.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Index => {
                if let Some(value) = ob {
                    let indexs: Vec<i64> = args.take()?;
                    let ret_value = if !value.i64_value.is_empty() {
                        let value = value.i64_value;
                        let mut ret_value = Vec::new();
                        for index in indexs {
                            let index = adjust_index_value(index, value.len())?;
                            ret_value.push(value[index]);
                        }
                        ret_value.into()
                    } else if !value.f64_value.is_empty() {
                        let value = value.f64_value;
                        let mut ret_value = Vec::new();
                        for index in indexs {
                            let index = adjust_index_value(index, value.len())?;
                            ret_value.push(value[index]);
                        }
                        ret_value.into()
                    } else if !value.blob_value.is_empty() {
                        let value = value.blob_value;
                        let mut ret_value = Vec::new();
                        for index in indexs {
                            let index = adjust_index_value(index, value.len())?;
                            ret_value.push(value[index].clone());
                        }
                        ret_value.into()
                    } else if !value.text_value.is_empty() {
                        let value = value.text_value;
                        let mut ret_value = Vec::new();
                        for index in indexs {
                            let index = adjust_index_value(index, value.len())?;
                            ret_value.push(value[index].clone());
                        }
                        ret_value.into()
                    } else {
                        ListValue::default()
                    };
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Range => {
                if let Some(value) = ob {
                    let range: (Bound<i64>, Bound<i64>) = args.take()?;
                    let ret_value = if !value.i64_value.is_empty() {
                        let value = value.i64_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else if !value.f64_value.is_empty() {
                        let value = value.f64_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else if !value.blob_value.is_empty() {
                        let value = value.blob_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else if !value.text_value.is_empty() {
                        let value = value.text_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else {
                        ListValue::default()
                    };
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Trim => {
                if let Some(value) = ob {
                    let range: (Bound<i64>, Bound<i64>) = args.take()?;
                    let new_value = if !value.i64_value.is_empty() {
                        let value = value.i64_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else if !value.f64_value.is_empty() {
                        let value = value.f64_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else if !value.blob_value.is_empty() {
                        let value = value.blob_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else if !value.text_value.is_empty() {
                        let value = value.text_value;
                        let range = adjust_range_bounds(range, value.len())?;
                        value[range].into()
                    } else {
                        ListValue::default()
                    };
                    wb.put(id, new_value.into());
                }
                Ok(().into())
            }
            Function::Lpop => {
                let count: i64 = args.take()?;
                if let Some(mut value) = ob {
                    let new_value = if !value.i64_value.is_empty() {
                        let value = &mut value.i64_value;
                        if value.len() > count as usize {
                            value.split_off(count as usize).into()
                        } else {
                            ListValue::default()
                        }
                    } else if !value.f64_value.is_empty() {
                        let value = &mut value.f64_value;
                        if value.len() > count as usize {
                            value.split_off(count as usize).into()
                        } else {
                            ListValue::default()
                        }
                    } else if !value.blob_value.is_empty() {
                        let value = &mut value.blob_value;
                        if value.len() > count as usize {
                            value.split_off(count as usize).into()
                        } else {
                            ListValue::default()
                        }
                    } else if !value.text_value.is_empty() {
                        let value = &mut value.text_value;
                        if value.len() > count as usize {
                            value.split_off(count as usize).into()
                        } else {
                            ListValue::default()
                        }
                    } else {
                        ListValue::default()
                    };
                    wb.put(id, new_value.into());
                    Ok(value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Rpop => {
                let count: i64 = args.take()?;
                if let Some(mut value) = ob {
                    let ret_value = if !value.i64_value.is_empty() {
                        let value = &mut value.i64_value;
                        if value.len() > count as usize {
                            value.split_off(value.len() - count as usize).into()
                        } else {
                            std::mem::take(value).into()
                        }
                    } else if !value.f64_value.is_empty() {
                        let value = &mut value.f64_value;
                        if value.len() > count as usize {
                            value.split_off(value.len() - count as usize).into()
                        } else {
                            std::mem::take(value).into()
                        }
                    } else if !value.blob_value.is_empty() {
                        let value = &mut value.blob_value;
                        if value.len() > count as usize {
                            value.split_off(value.len() - count as usize).into()
                        } else {
                            std::mem::take(value).into()
                        }
                    } else if !value.text_value.is_empty() {
                        let value = &mut value.text_value;
                        if value.len() > count as usize {
                            value.split_off(value.len() - count as usize).into()
                        } else {
                            std::mem::take(value).into()
                        }
                    } else {
                        ListValue::default()
                    };
                    wb.put(id, value.into());
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Lpush => {
                let new_value = if let Some(value) = ob {
                    if !value.i64_value.is_empty() {
                        let mut new_value: Vec<i64> = args.take()?;
                        new_value.extend(value.i64_value);
                        new_value.into()
                    } else if !value.f64_value.is_empty() {
                        let mut new_value: Vec<f64> = args.take()?;
                        new_value.extend(value.f64_value);
                        new_value.into()
                    } else if !value.blob_value.is_empty() {
                        let mut new_value: Vec<Vec<u8>> = args.take()?;
                        new_value.extend(value.blob_value);
                        new_value.into()
                    } else if !value.text_value.is_empty() {
                        let mut new_value: Vec<String> = args.take()?;
                        new_value.extend(value.text_value);
                        new_value.into()
                    } else {
                        ListValue::default()
                    }
                } else {
                    args.take()?
                };
                wb.put(id, new_value.into());
                Ok(().into())
            }
            Function::Rpush => {
                let new_value = if let Some(mut value) = ob {
                    if !value.i64_value.is_empty() {
                        let operand: Vec<i64> = args.take()?;
                        value.i64_value.extend(operand);
                    } else if !value.f64_value.is_empty() {
                        let operand: Vec<f64> = args.take()?;
                        value.f64_value.extend(operand);
                    } else if !value.blob_value.is_empty() {
                        let operand: Vec<Vec<u8>> = args.take()?;
                        value.blob_value.extend(operand);
                    } else if !value.text_value.is_empty() {
                        let operand: Vec<String> = args.take()?;
                        value.text_value.extend(operand);
                    }
                    value
                } else {
                    args.take()?
                };
                wb.put(id, new_value.into());
                Ok(().into())
            }
            _ => Err(Error::invalid_argument("unsupported list function")),
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

fn adjust_index_value(i: i64, len: usize) -> Result<usize> {
    let len = i64::try_from(len).map_err(|_| Error::internal("convert usize to i64"))?;
    let i = if i < 0 {
        if let Some(i) = i.checked_add(len) {
            i
        } else {
            return Err(Error::invalid_argument("index overflow"));
        }
    } else {
        i
    };
    if i < 0 || i >= len {
        return Err(Error::invalid_argument("index out of range"));
    };
    usize::try_from(i).map_err(|_| Error::invalid_argument("convert i64 to usize"))
}

fn adjust_bound_value(i: i64, len: i64) -> Result<usize> {
    let i = if i < 0 {
        if let Some(i) = i.checked_add(len) {
            if i < 0 {
                0
            } else {
                i
            }
        } else {
            return Err(Error::invalid_argument("range bound overflow"));
        }
    } else {
        i
    };
    usize::try_from(i).map_err(|_| Error::invalid_argument("convert i64 to usize"))
}

fn adjust_range_bound(bound: Bound<i64>, len: usize) -> Result<Bound<usize>> {
    let len = i64::try_from(len).map_err(|_| Error::internal("convert usize to i64"))?;
    let bound = match bound {
        Bound::Included(i) => {
            let i = adjust_bound_value(i, len)?;
            Bound::Included(i)
        }
        Bound::Excluded(i) => {
            let i = adjust_bound_value(i, len)?;
            Bound::Excluded(i)
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    Ok(bound)
}

fn adjust_range_bounds(
    range: (Bound<i64>, Bound<i64>),
    len: usize,
) -> Result<(Bound<usize>, Bound<usize>)> {
    let start = adjust_range_bound(range.0, len)?;
    let end = adjust_range_bound(range.1, len)?;
    Ok((start, end))
}
