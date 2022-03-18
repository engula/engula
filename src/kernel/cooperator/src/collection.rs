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
    collections::HashMap,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use engula_apis::v1::*;
use tokio::sync::Mutex;

use crate::{Args, Error, Result, Write, WriteBatch};

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
            Expr::MapExpr(expr) => self.execute_map_expr(wb, id, expr).await,
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
                    let ret_value = list_lpop(&mut value, count)?;
                    wb.put(id, value.into());
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Rpop => {
                let count: i64 = args.take()?;
                if let Some(mut value) = ob {
                    let ret_value = list_rpop(&mut value, count)?;
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

    async fn execute_map_expr(
        &self,
        wb: &mut WriteBatch,
        id: Vec<u8>,
        expr: MapExpr,
    ) -> Result<TypedValue> {
        let (func, mut args) = parse_call_expr(expr.call)?;
        let ob: Option<MapValue> = self.get(&id).await?;
        match func {
            Function::Len => {
                if let Some(map) = ob {
                    let len = if let Some(keys) = map.keys {
                        List(keys).len()?
                    } else {
                        0
                    };
                    Ok(len.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Index => {
                if let Some(map) = ob {
                    let ret_value = if let (Some(keys), Some(values)) = (map.keys, map.values) {
                        let target: ListValue = args.take()?;
                        let (keys, indexs) = List(keys).find(target)?;
                        let values = List(values).remove(&indexs)?;
                        (keys, values).into()
                    } else {
                        MapValue::default()
                    };
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Range => {
                if let Some(map) = ob {
                    let ret_value = if let (Some(keys), Some(values)) = (map.keys, map.values) {
                        let range: RangeValue = args.take()?;
                        let (keys, indexs) = List(keys).scan(range)?;
                        let values = List(values).remove(&indexs)?;
                        (keys, values).into()
                    } else {
                        MapValue::default()
                    };
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Contains => {
                if let Some(map) = ob {
                    let ret_value = if let Some(keys) = map.keys {
                        let target: ListValue = args.take()?;
                        let (keys, _) = List(keys).find(target)?;
                        keys
                    } else {
                        ListValue::default()
                    };
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Clear => {
                if ob.is_some() {
                    wb.put(id, MapValue::default().into());
                }
                Ok(().into())
            }
            Function::Extend => {
                let operand: MapValue = args.take()?;
                match (operand.keys, operand.values) {
                    (Some(new_keys), Some(new_values)) => {
                        let map = ob.unwrap_or_default();
                        let mut keys = List(map.keys.unwrap_or_default());
                        let mut values = List(map.values.unwrap_or_default());
                        keys.rpush(new_keys)?;
                        values.rpush(new_values)?;
                        let map: MapValue = (keys.0, values.0).into();
                        wb.put(id, map.into());
                        Ok(().into())
                    }
                    (None, None) => Ok(().into()),
                    _ => Err(Error::invalid_argument("missing keys or values")),
                }
            }
            Function::Remove => {
                if let Some(map) = ob {
                    match (map.keys, map.values) {
                        (Some(keys), Some(values)) => {
                            let target: ListValue = args.take()?;
                            let mut keys = List(keys);
                            let mut values = List(values);
                            let (_, indexs) = keys.find(target)?;
                            keys.remove(&indexs)?;
                            values.remove(&indexs)?;
                            let map: MapValue = (keys.0, values.0).into();
                            wb.put(id, map.into());
                            Ok(().into())
                        }
                        (None, None) => Ok(().into()),
                        _ => Err(Error::internal("missing keys or values")),
                    }
                } else {
                    Ok(().into())
                }
            }
            _ => Err(Error::invalid_argument("unsupported map function")),
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
                if let Some(list) = ob {
                    let len = List(list).len()?;
                    Ok(len.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Index => {
                if let Some(list) = ob {
                    let indexs: Vec<i64> = args.take()?;
                    let ret_value = List(list).remove(&indexs)?;
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Range => {
                if let Some(list) = ob {
                    let range: (Bound<i64>, Bound<i64>) = args.take()?;
                    let ret_value = List(list).range(range)?;
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Trim => {
                if let Some(list) = ob {
                    let range: (Bound<i64>, Bound<i64>) = args.take()?;
                    let new_list = List(list).range(range)?;
                    wb.put(id, new_list.into());
                }
                Ok(().into())
            }
            Function::Lpop => {
                let count: i64 = args.take()?;
                if let Some(list) = ob {
                    let mut list = List(list);
                    let ret_value = list.lpop(count)?;
                    wb.put(id, list.0.into());
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Rpop => {
                let count: i64 = args.take()?;
                if let Some(list) = ob {
                    let mut list = List(list);
                    let ret_value = list.rpop(count)?;
                    wb.put(id, list.0.into());
                    Ok(ret_value.into())
                } else {
                    Ok(().into())
                }
            }
            Function::Lpush => {
                let operand: ListValue = args.take()?;
                let mut list = List(ob.unwrap_or_default());
                list.lpush(operand)?;
                wb.put(id, list.0.into());
                Ok(().into())
            }
            Function::Rpush => {
                let operand: ListValue = args.take()?;
                let mut list = List(ob.unwrap_or_default());
                list.rpush(operand)?;
                wb.put(id, list.0.into());
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

struct List(ListValue);

impl List {
    fn len(&self) -> Result<i64> {
        let len = if !self.0.i64_value.is_empty() {
            self.0.i64_value.len()
        } else if !self.0.f64_value.is_empty() {
            self.0.f64_value.len()
        } else if !self.0.blob_value.is_empty() {
            self.0.blob_value.len()
        } else if !self.0.text_value.is_empty() {
            self.0.text_value.len()
        } else {
            0
        };
        i64::try_from(len).map_err(|_| Error::internal("convert usize to i64"))
    }

    fn find(&self, values: ListValue) -> Result<(ListValue, Vec<i64>)> {
        if !self.0.i64_value.is_empty() {
            list_find(&self.0.i64_value, values)
        } else if !self.0.blob_value.is_empty() {
            list_find(&self.0.blob_value, values)
        } else if !self.0.text_value.is_empty() {
            list_find(&self.0.text_value, values)
        } else {
            Ok((ListValue::default(), Vec::new()))
        }
    }

    fn scan(self, range: RangeValue) -> Result<(ListValue, Vec<i64>)> {
        if !self.0.i64_value.is_empty() {
            list_scan(self.0.i64_value, range)
        } else if !self.0.blob_value.is_empty() {
            list_scan(self.0.blob_value, range)
        } else if !self.0.text_value.is_empty() {
            list_scan(self.0.text_value, range)
        } else {
            Ok((ListValue::default(), Vec::new()))
        }
    }

    fn range(self, range: (Bound<i64>, Bound<i64>)) -> Result<ListValue> {
        let ret_value = if !self.0.i64_value.is_empty() {
            let value = self.0.i64_value;
            let range = adjust_range_bounds(range, value.len())?;
            value[range].into()
        } else if !self.0.f64_value.is_empty() {
            let value = self.0.f64_value;
            let range = adjust_range_bounds(range, value.len())?;
            value[range].into()
        } else if !self.0.blob_value.is_empty() {
            let value = self.0.blob_value;
            let range = adjust_range_bounds(range, value.len())?;
            value[range].into()
        } else if !self.0.text_value.is_empty() {
            let value = self.0.text_value;
            let range = adjust_range_bounds(range, value.len())?;
            value[range].into()
        } else {
            ListValue::default()
        };
        Ok(ret_value)
    }

    fn lpop(&mut self, count: i64) -> Result<ListValue> {
        let ret_value = if !self.0.i64_value.is_empty() {
            list_lpop(&mut self.0.i64_value, count)?.into()
        } else if !self.0.f64_value.is_empty() {
            list_lpop(&mut self.0.f64_value, count)?.into()
        } else if !self.0.blob_value.is_empty() {
            list_lpop(&mut self.0.blob_value, count)?.into()
        } else if !self.0.text_value.is_empty() {
            list_lpop(&mut self.0.text_value, count)?.into()
        } else {
            ListValue::default()
        };
        Ok(ret_value)
    }

    fn rpop(&mut self, count: i64) -> Result<ListValue> {
        let ret_value = if !self.0.i64_value.is_empty() {
            list_rpop(&mut self.0.i64_value, count)?.into()
        } else if !self.0.f64_value.is_empty() {
            list_rpop(&mut self.0.f64_value, count)?.into()
        } else if !self.0.blob_value.is_empty() {
            list_rpop(&mut self.0.blob_value, count)?.into()
        } else if !self.0.text_value.is_empty() {
            list_rpop(&mut self.0.text_value, count)?.into()
        } else {
            ListValue::default()
        };
        Ok(ret_value)
    }

    fn lpush(&mut self, values: ListValue) -> Result<()> {
        if !self.0.i64_value.is_empty() {
            list_lpush(&mut self.0.i64_value, values)
        } else if !self.0.f64_value.is_empty() {
            list_lpush(&mut self.0.f64_value, values)
        } else if !self.0.blob_value.is_empty() {
            list_lpush(&mut self.0.blob_value, values)
        } else if !self.0.text_value.is_empty() {
            list_lpush(&mut self.0.text_value, values)
        } else {
            self.0 = values;
            Ok(())
        }
    }

    fn rpush(&mut self, values: ListValue) -> Result<()> {
        if !self.0.i64_value.is_empty() {
            list_rpush(&mut self.0.i64_value, values)
        } else if !self.0.f64_value.is_empty() {
            list_rpush(&mut self.0.f64_value, values)
        } else if !self.0.blob_value.is_empty() {
            list_rpush(&mut self.0.blob_value, values)
        } else if !self.0.text_value.is_empty() {
            list_rpush(&mut self.0.text_value, values)
        } else {
            self.0 = values;
            Ok(())
        }
    }

    fn remove(&mut self, indexs: &[i64]) -> Result<ListValue> {
        let ret_value = if !self.0.i64_value.is_empty() {
            list_remove(&mut self.0.i64_value, indexs)?.into()
        } else if !self.0.f64_value.is_empty() {
            list_remove(&mut self.0.f64_value, indexs)?.into()
        } else if !self.0.blob_value.is_empty() {
            list_remove(&mut self.0.blob_value, indexs)?.into()
        } else if !self.0.text_value.is_empty() {
            list_remove(&mut self.0.text_value, indexs)?.into()
        } else {
            ListValue::default()
        };
        Ok(ret_value)
    }
}

fn list_find<T>(list: &[T], values: ListValue) -> Result<(ListValue, Vec<i64>)>
where
    T: Eq,
    Vec<T>: TryFrom<ListValue, Error = ListValue> + Into<ListValue>,
{
    let values: Vec<T> = values
        .try_into()
        .map_err(|_| Error::invalid_argument("index type mismatch"))?;
    let mut found_values = Vec::new();
    let mut found_indexs = Vec::new();
    for value in values {
        if let Some(i) = list.iter().position(|v| v == &value) {
            let index = i64::try_from(i).map_err(|_| Error::internal("convert usize to i64"))?;
            found_values.push(value);
            found_indexs.push(index);
        }
    }
    Ok((found_values.into(), found_indexs))
}

fn list_scan<T>(list: Vec<T>, range: RangeValue) -> Result<(ListValue, Vec<i64>)>
where
    T: Ord,
    Vec<T>: Into<ListValue>,
    (Bound<T>, Bound<T>): TryFrom<RangeValue>,
{
    let range: (Bound<T>, Bound<T>) = range
        .try_into()
        .map_err(|_| Error::invalid_argument("range type mismatch"))?;
    let mut found_values = Vec::new();
    let mut found_indexs = Vec::new();
    for (index, value) in list.into_iter().enumerate() {
        if range.contains(&value) {
            let index =
                i64::try_from(index).map_err(|_| Error::internal("convert usize to i64"))?;
            found_values.push(value);
            found_indexs.push(index);
        }
    }
    Ok((found_values.into(), found_indexs))
}
fn list_lpop<T>(list: &mut Vec<T>, count: i64) -> Result<Vec<T>> {
    let count =
        usize::try_from(count).map_err(|_| Error::invalid_argument("convert i64 to usize"))?;
    let mut values = if list.len() > count {
        list.split_off(count)
    } else {
        Vec::new()
    };
    std::mem::swap(list, &mut values);
    Ok(values)
}

fn list_rpop<T>(list: &mut Vec<T>, count: i64) -> Result<Vec<T>> {
    let count =
        usize::try_from(count).map_err(|_| Error::invalid_argument("convert i64 to usize"))?;
    let values = if list.len() > count {
        list.split_off(list.len() - count)
    } else {
        std::mem::take(list)
    };
    Ok(values)
}

fn list_lpush<T>(list: &mut Vec<T>, values: ListValue) -> Result<()>
where
    Vec<T>: TryFrom<ListValue>,
{
    let mut values: Vec<T> = values
        .try_into()
        .map_err(|_| Error::invalid_argument("value type mismatch"))?;
    values.append(list);
    std::mem::swap(list, &mut values);
    Ok(())
}

fn list_rpush<T>(list: &mut Vec<T>, values: ListValue) -> Result<()>
where
    Vec<T>: TryFrom<ListValue>,
{
    let values: Vec<T> = values
        .try_into()
        .map_err(|_| Error::invalid_argument("value type mismatch"))?;
    list.extend(values);
    Ok(())
}

fn list_remove<T>(list: &mut Vec<T>, indexs: &[i64]) -> Result<Vec<T>> {
    let mut values = Vec::new();
    for index in indexs {
        let len = list.len() + values.len();
        let index = adjust_index_value(*index, len)?;
        let index = index
            .checked_sub(values.len())
            .ok_or_else(|| Error::invalid_argument("index out of range"))?;
        values.push(list.remove(index));
    }
    Ok(values)
}
