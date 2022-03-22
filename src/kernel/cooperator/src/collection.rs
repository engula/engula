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
use prost::Message;
use tokio::sync::Mutex;

use crate::{Args, Error, Result, Write, WriteBatch};

#[derive(Clone)]
pub struct Collection {
    objects: Arc<Mutex<HashMap<Vec<u8>, Value>>>,
}

impl Collection {
    pub fn new(_: CollectionDesc) -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get<T: TryFrom<Value>>(&self, id: &[u8]) -> Result<T> {
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
        let mut res = CollectionResponse::default();
        for expr in req.exprs {
            let result = self.execute_expr(wb, expr).await?;
            res.results.push(result);
        }
        Ok(res)
    }

    async fn execute_expr(&self, wb: &mut WriteBatch, expr: ObjectExpr) -> Result<ObjectResult> {
        let mut result = ObjectResult::default();
        if let Some(select) = expr.select {
            for id in expr.batch {
                let value = self.execute_select(id, select.clone()).await?;
                result.values.push(value);
            }
        } else if let Some(mutate) = expr.mutate {
            for id in expr.batch {
                let value = self.execute_mutate(wb, id, mutate.clone()).await?;
                result.values.push(value);
            }
        }
        Ok(result)
    }

    async fn execute_select(&self, id: Vec<u8>, expr: SelectExpr) -> Result<Value> {
        let ob: Value = self.get(&id).await?;
        let func = SelectFunction::from_i32(expr.func).unwrap_or_default();
        match func {
            SelectFunction::Get => {
                if let Some(value) = ob.value {
                    let ret_value: Value = match value {
                        value::Value::I64Value(v) => v.into(),
                        value::Value::F64Value(v) => v.into(),
                        value::Value::BlobValue(v) => {
                            if let Some(index) = expr.index {
                                let range: (Bound<i64>, Bound<i64>) = index
                                    .try_into()
                                    .map_err(|_| Error::invalid_argument("invalid range index"))?;
                                let range = adjust_range_bounds(range, v.len())?;
                                v[range].into()
                            } else {
                                v.into()
                            }
                        }
                        value::Value::TextValue(v) => {
                            if expr.index.is_some() {
                                todo!()
                            } else {
                                v.into()
                            }
                        }
                        value::Value::ListValue(v) => {
                            if let Some(index) = expr.index {
                                let index = index.value.ok_or_else(|| {
                                    Error::invalid_argument("missing index value")
                                })?;
                                match index {
                                    value::Value::I64Value(x) => List(v).get(x)?,
                                    value::Value::ListValue(x) => {
                                        let batch: Vec<i64> = x.try_into().map_err(|_| {
                                            Error::invalid_argument("invalid batch index")
                                        })?;
                                        List(v).get_batch(&batch)?.into()
                                    }
                                    value::Value::RangeValue(x) => {
                                        let range: (Bound<i64>, Bound<i64>) =
                                            x.try_into().map_err(|_| {
                                                Error::invalid_argument("invalid range index")
                                            })?;
                                        List(v).get_range(range)?.into()
                                    }
                                    _ => return Err(Error::invalid_argument("invalid index")),
                                }
                            } else {
                                v.into()
                            }
                        }
                        value::Value::MapValue(v) => {
                            if let Some(index) = expr.index {
                                let keys = v.keys.ok_or_else(|| Error::internal("missing keys"))?;
                                let values =
                                    v.values.ok_or_else(|| Error::internal("missing values"))?;
                                let index = index.value.ok_or_else(|| {
                                    Error::invalid_argument("missing index value")
                                })?;
                                match index {
                                    value::Value::ListValue(x) => {
                                        let (keys, indexs) = List(keys).find_batch(x)?;
                                        let values = List(values).get_batch(&indexs)?;
                                        MapValue::from((keys, values)).into()
                                    }
                                    value::Value::RangeValue(x) => {
                                        let (keys, indexs) = List(keys).find_range(x)?;
                                        let values = List(values).get_batch(&indexs)?;
                                        MapValue::from((keys, values)).into()
                                    }
                                    x => {
                                        let index = List(keys).find(x.into())?;
                                        if let Some(index) = index {
                                            List(values).get(index)?
                                        } else {
                                            ().into()
                                        }
                                    }
                                }
                            } else {
                                v.into()
                            }
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    };
                    Ok(ret_value)
                } else {
                    Ok(().into())
                }
            }
            SelectFunction::Len => {
                if let Some(value) = ob.value {
                    let len = match value {
                        value::Value::BlobValue(v) => v.len(),
                        value::Value::TextValue(v) => v.len(),
                        value::Value::ListValue(v) => List(v).len(),
                        value::Value::MapValue(v) => {
                            let keys = v.keys.ok_or_else(|| Error::internal("missing keys"))?;
                            List(keys).len()
                        }
                        value::Value::SetValue(v) => {
                            let keys = v.keys.ok_or_else(|| Error::internal("missing keys"))?;
                            List(keys).len()
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    };
                    let len =
                        i64::try_from(len).map_err(|_| Error::internal("convert usize to i64"))?;
                    Ok(len.into())
                } else {
                    Ok(().into())
                }
            }
        }
    }

    async fn execute_mutate(
        &self,
        wb: &mut WriteBatch,
        id: Vec<u8>,
        expr: MutateExpr,
    ) -> Result<Value> {
        let func = MutateFunction::from_i32(expr.func).unwrap_or_default();
        let mut args = Args::new(expr.args);
        match func {
            MutateFunction::Set => {
                let value: Value = args.take()?;
                wb.put(id, value);
                Ok(().into())
            }
            MutateFunction::Delete => {
                wb.delete(id);
                Ok(().into())
            }
            MutateFunction::Add => {
                let ob: Value = self.get(&id).await?;
                let new_value = if let Some(value) = ob.value {
                    match value {
                        value::Value::I64Value(v) => {
                            let operand: i64 = args.take()?;
                            (v + operand).into()
                        }
                        value::Value::F64Value(v) => {
                            let operand: f64 = args.take()?;
                            (v + operand).into()
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                } else {
                    let operand: value::Value = args.take()?;
                    match operand {
                        value::Value::I64Value(_) | value::Value::F64Value(_) => operand,
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                };
                wb.put(id, new_value.into());
                Ok(().into())
            }
            MutateFunction::Trim => {
                let ob: Value = self.get(&id).await?;
                let index = expr
                    .index
                    .ok_or_else(|| Error::invalid_argument("missing index"))?;
                let range: (Bound<i64>, Bound<i64>) = index
                    .try_into()
                    .map_err(|_| Error::invalid_argument("invalid range index"))?;
                if let Some(value) = ob.value {
                    match value {
                        value::Value::BlobValue(v) => {
                            let range = adjust_range_bounds(range, v.len())?;
                            let new_value = v[range].into();
                            wb.put(id, new_value);
                        }
                        value::Value::ListValue(v) => {
                            let new_value = List(v).get_range(range)?;
                            wb.put(id, new_value.into());
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                }
                Ok(().into())
            }
            MutateFunction::Lpop => {
                let ob: Value = self.get(&id).await?;
                let count: i64 = args.take()?;
                if let Some(value) = ob.value {
                    match value {
                        value::Value::BlobValue(mut v) => {
                            let ret_value = list_lpop(&mut v, count)?;
                            wb.put(id, v.into());
                            Ok(ret_value.into())
                        }
                        value::Value::ListValue(v) => {
                            let mut list = List(v);
                            let ret_value = list.lpop(count)?;
                            wb.put(id, list.0.into());
                            Ok(ret_value.into())
                        }
                        _ => Err(Error::invalid_argument("unsupported object")),
                    }
                } else {
                    Ok(().into())
                }
            }
            MutateFunction::Rpop => {
                let ob: Value = self.get(&id).await?;
                let count: i64 = args.take()?;
                if let Some(value) = ob.value {
                    match value {
                        value::Value::BlobValue(mut v) => {
                            let ret_value = list_rpop(&mut v, count)?;
                            wb.put(id, v.into());
                            Ok(ret_value.into())
                        }
                        value::Value::ListValue(v) => {
                            let mut list = List(v);
                            let ret_value = list.rpop(count)?;
                            wb.put(id, list.0.into());
                            Ok(ret_value.into())
                        }
                        _ => Err(Error::invalid_argument("unsupported object")),
                    }
                } else {
                    Ok(().into())
                }
            }
            MutateFunction::Lpush => {
                let ob: Value = self.get(&id).await?;
                if let Some(value) = ob.value {
                    match value {
                        value::Value::BlobValue(v) => {
                            let mut operand: Vec<u8> = args.take()?;
                            operand.extend(v);
                            wb.put(id, operand.into());
                        }
                        value::Value::ListValue(v) => {
                            let operand: ListValue = args.take()?;
                            let mut list = List(v);
                            list.lpush(operand)?;
                            wb.put(id, list.0.into());
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                } else {
                    let value: value::Value = args.take()?;
                    match value {
                        v @ (value::Value::BlobValue(_) | value::Value::ListValue(_)) => {
                            wb.put(id, v.into());
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                }
                Ok(().into())
            }
            MutateFunction::Rpush => {
                let ob: Value = self.get(&id).await?;
                if let Some(value) = ob.value {
                    match value {
                        value::Value::BlobValue(mut v) => {
                            let operand: Vec<u8> = args.take()?;
                            v.extend(operand);
                            wb.put(id, v.into());
                        }
                        value::Value::ListValue(v) => {
                            let operand: ListValue = args.take()?;
                            let mut list = List(v);
                            list.rpush(operand)?;
                            wb.put(id, list.0.into());
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                } else {
                    let value: value::Value = args.take()?;
                    match value {
                        v @ (value::Value::BlobValue(_) | value::Value::ListValue(_)) => {
                            wb.put(id, v.into());
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                }
                Ok(().into())
            }
            MutateFunction::Clear => {
                let ob: Value = self.get(&id).await?;
                if let Some(value) = ob.value {
                    let new_value = match value {
                        value::Value::ListValue(_) => ListValue::default().into(),
                        value::Value::MapValue(_) => MapValue::default().into(),
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    };
                    wb.put(id, new_value);
                }
                Ok(().into())
            }
            MutateFunction::Extend => {
                let ob: Value = self.get(&id).await?;
                if let Some(value) = ob.value {
                    match value {
                        value::Value::MapValue(v) => {
                            let keys = v.keys.ok_or_else(|| Error::internal("missing keys"))?;
                            let values =
                                v.values.ok_or_else(|| Error::internal("missing values"))?;
                            let mut keys = List(keys);
                            let mut values = List(values);
                            let operand: MapValue = args.take()?;
                            let new_keys = operand
                                .keys
                                .ok_or_else(|| Error::invalid_argument("missing keys"))?;
                            let new_values = operand
                                .values
                                .ok_or_else(|| Error::invalid_argument("missing values"))?;
                            keys.lpush(new_keys)?;
                            values.lpush(new_values)?;
                            wb.put(id, MapValue::from((keys.0, values.0)).into());
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                } else {
                    let operand: MapValue = args.take()?;
                    wb.put(id, operand.into());
                }
                Ok(().into())
            }
            MutateFunction::Remove => {
                let index = expr
                    .index
                    .and_then(|x| x.value)
                    .ok_or_else(|| Error::invalid_argument("missing index"))?;
                let ob: Value = self.get(&id).await?;
                if let Some(value) = ob.value {
                    match value {
                        value::Value::MapValue(v) => {
                            let keys = v.keys.ok_or_else(|| Error::internal("missing keys"))?;
                            let values =
                                v.values.ok_or_else(|| Error::internal("missing values"))?;
                            let mut keys = List(keys);
                            let mut values = List(values);
                            match index {
                                value::Value::ListValue(x) => {
                                    let (_, indexs) = keys.find_batch(x)?;
                                    keys.remove_batch(&indexs)?;
                                    values.remove_batch(&indexs)?;
                                    wb.put(id, MapValue::from((keys.0, values.0)).into());
                                }
                                value::Value::RangeValue(x) => {
                                    let (_, indexs) = keys.find_range(x)?;
                                    keys.remove_batch(&indexs)?;
                                    values.remove_batch(&indexs)?;
                                    wb.put(id, MapValue::from((keys.0, values.0)).into());
                                }
                                x => {
                                    let index = keys.find(x.into())?;
                                    if let Some(index) = index {
                                        keys.remove(index)?;
                                        values.remove(index)?;
                                        wb.put(id, MapValue::from((keys.0, values.0)).into());
                                    }
                                }
                            }
                        }
                        _ => return Err(Error::invalid_argument("unsupported object")),
                    }
                }
                Ok(().into())
            }
        }
    }
}

struct List(ListValue);

impl List {
    fn len(&self) -> usize {
        if !self.0.i64_value.is_empty() {
            self.0.i64_value.len()
        } else if !self.0.f64_value.is_empty() {
            self.0.f64_value.len()
        } else if !self.0.blob_value.is_empty() {
            self.0.blob_value.len()
        } else if !self.0.text_value.is_empty() {
            self.0.text_value.len()
        } else {
            0
        }
    }

    fn get(self, index: i64) -> Result<Value> {
        let ret_value = if !self.0.i64_value.is_empty() {
            list_get(&self.0.i64_value, index)?.into()
        } else if !self.0.f64_value.is_empty() {
            list_get(&self.0.f64_value, index)?.into()
        } else if !self.0.blob_value.is_empty() {
            list_get(&self.0.blob_value, index)?.into()
        } else if !self.0.text_value.is_empty() {
            list_get(&self.0.text_value, index)?.into()
        } else {
            Value::default()
        };
        Ok(ret_value)
    }

    fn get_batch(self, batch: &[i64]) -> Result<ListValue> {
        let ret_value = if !self.0.i64_value.is_empty() {
            list_get_batch(&self.0.i64_value, batch)?.into()
        } else if !self.0.f64_value.is_empty() {
            list_get_batch(&self.0.f64_value, batch)?.into()
        } else if !self.0.blob_value.is_empty() {
            list_get_batch(&self.0.blob_value, batch)?.into()
        } else if !self.0.text_value.is_empty() {
            list_get_batch(&self.0.text_value, batch)?.into()
        } else {
            ListValue::default()
        };
        Ok(ret_value)
    }

    fn get_range(self, range: (Bound<i64>, Bound<i64>)) -> Result<ListValue> {
        let ret_value = if !self.0.i64_value.is_empty() {
            list_get_range(&self.0.i64_value, range)?.into()
        } else if !self.0.f64_value.is_empty() {
            list_get_range(&self.0.f64_value, range)?.into()
        } else if !self.0.blob_value.is_empty() {
            list_get_range(&self.0.blob_value, range)?.into()
        } else if !self.0.text_value.is_empty() {
            list_get_range(&self.0.text_value, range)?.into()
        } else {
            ListValue::default()
        };
        Ok(ret_value)
    }

    fn find(&self, value: Value) -> Result<Option<i64>> {
        if !self.0.i64_value.is_empty() {
            list_find(&self.0.i64_value, value)
        } else if !self.0.blob_value.is_empty() {
            list_find(&self.0.blob_value, value)
        } else if !self.0.text_value.is_empty() {
            list_find(&self.0.text_value, value)
        } else if self.0.encoded_len() != 0 {
            Err(Error::invalid_argument("unsupported object"))
        } else {
            Ok(None)
        }
    }

    fn find_batch(&self, batch: ListValue) -> Result<(ListValue, Vec<i64>)> {
        if !self.0.i64_value.is_empty() {
            list_find_batch(&self.0.i64_value, batch)
        } else if !self.0.blob_value.is_empty() {
            list_find_batch(&self.0.blob_value, batch)
        } else if !self.0.text_value.is_empty() {
            list_find_batch(&self.0.text_value, batch)
        } else if self.0.encoded_len() != 0 {
            Err(Error::invalid_argument("unsupported object"))
        } else {
            Ok((ListValue::default(), Vec::new()))
        }
    }

    fn find_range(&self, range: RangeValue) -> Result<(ListValue, Vec<i64>)> {
        if !self.0.i64_value.is_empty() {
            list_find_range(&self.0.i64_value, range)
        } else if !self.0.blob_value.is_empty() {
            list_find_range(&self.0.blob_value, range)
        } else if !self.0.text_value.is_empty() {
            list_find_range(&self.0.text_value, range)
        } else if self.0.encoded_len() != 0 {
            Err(Error::invalid_argument("unsupported object"))
        } else {
            Ok((ListValue::default(), Vec::new()))
        }
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

    fn lpush(&mut self, value: ListValue) -> Result<()> {
        if !self.0.i64_value.is_empty() {
            list_lpush(&mut self.0.i64_value, value)
        } else if !self.0.f64_value.is_empty() {
            list_lpush(&mut self.0.f64_value, value)
        } else if !self.0.blob_value.is_empty() {
            list_lpush(&mut self.0.blob_value, value)
        } else if !self.0.text_value.is_empty() {
            list_lpush(&mut self.0.text_value, value)
        } else {
            self.0 = value;
            Ok(())
        }
    }

    fn rpush(&mut self, value: ListValue) -> Result<()> {
        if !self.0.i64_value.is_empty() {
            list_rpush(&mut self.0.i64_value, value)
        } else if !self.0.f64_value.is_empty() {
            list_rpush(&mut self.0.f64_value, value)
        } else if !self.0.blob_value.is_empty() {
            list_rpush(&mut self.0.blob_value, value)
        } else if !self.0.text_value.is_empty() {
            list_rpush(&mut self.0.text_value, value)
        } else {
            self.0 = value;
            Ok(())
        }
    }

    fn remove(&mut self, index: i64) -> Result<()> {
        self.remove_batch(&[index])
    }

    fn remove_batch(&mut self, batch: &[i64]) -> Result<()> {
        if !self.0.i64_value.is_empty() {
            list_remove_batch(&mut self.0.i64_value, batch)?;
        } else if !self.0.f64_value.is_empty() {
            list_remove_batch(&mut self.0.f64_value, batch)?;
        } else if !self.0.blob_value.is_empty() {
            list_remove_batch(&mut self.0.blob_value, batch)?;
        } else if !self.0.text_value.is_empty() {
            list_remove_batch(&mut self.0.text_value, batch)?;
        }
        Ok(())
    }
}

fn list_get<T: Clone>(list: &[T], index: i64) -> Result<T> {
    let index = adjust_index_value(index, list.len())?;
    Ok(list[index].clone())
}

fn list_get_batch<T: Clone>(list: &[T], batch: &[i64]) -> Result<Vec<T>> {
    let mut elems = Vec::new();
    for i in batch {
        let index = adjust_index_value(*i, list.len())?;
        elems.push(list[index].clone());
    }
    Ok(elems)
}

fn list_get_range<T: Clone>(list: &[T], range: (Bound<i64>, Bound<i64>)) -> Result<Vec<T>> {
    let range = adjust_range_bounds(range, list.len())?;
    Ok(list[range].to_vec())
}

fn list_find<T>(list: &[T], value: Value) -> Result<Option<i64>>
where
    T: Eq + TryFrom<Value, Error = Value>,
{
    let value = value
        .try_into()
        .map_err(|_| Error::invalid_argument("value type mismatch"))?;
    if let Some(i) = list.iter().position(|v| v == &value) {
        let index = i64::try_from(i).map_err(|_| Error::internal("convert usize to i64"))?;
        Ok(Some(index))
    } else {
        Ok(None)
    }
}

fn list_find_batch<T>(list: &[T], batch: ListValue) -> Result<(ListValue, Vec<i64>)>
where
    T: Eq + Clone,
    Vec<T>: TryFrom<ListValue, Error = ListValue> + Into<ListValue>,
{
    let batch: Vec<T> = batch
        .try_into()
        .map_err(|_| Error::invalid_argument("value type mismatch"))?;
    let mut found_values = Vec::new();
    let mut found_indexs = Vec::new();
    for value in batch {
        if let Some(i) = list.iter().position(|v| v == &value) {
            let index = i64::try_from(i).map_err(|_| Error::internal("convert usize to i64"))?;
            found_values.push(value.clone());
            found_indexs.push(index);
        }
    }
    Ok((found_values.into(), found_indexs))
}

fn list_find_range<T>(list: &[T], range: RangeValue) -> Result<(ListValue, Vec<i64>)>
where
    T: Ord + Clone,
    Vec<T>: Into<ListValue>,
    (Bound<T>, Bound<T>): TryFrom<RangeValue>,
{
    let range: (Bound<T>, Bound<T>) = range
        .try_into()
        .map_err(|_| Error::invalid_argument("range type mismatch"))?;
    let mut found_values = Vec::new();
    let mut found_indexs = Vec::new();
    for (index, value) in list.iter().enumerate() {
        if range.contains(value) {
            let index =
                i64::try_from(index).map_err(|_| Error::internal("convert usize to i64"))?;
            found_values.push(value.clone());
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

fn list_lpush<T>(list: &mut Vec<T>, value: ListValue) -> Result<()>
where
    Vec<T>: TryFrom<ListValue>,
{
    let mut value: Vec<T> = value
        .try_into()
        .map_err(|_| Error::invalid_argument("value type mismatch"))?;
    value.append(list);
    std::mem::swap(list, &mut value);
    Ok(())
}

fn list_rpush<T>(list: &mut Vec<T>, value: ListValue) -> Result<()>
where
    Vec<T>: TryFrom<ListValue>,
{
    let value: Vec<T> = value
        .try_into()
        .map_err(|_| Error::invalid_argument("value type mismatch"))?;
    list.extend(value);
    Ok(())
}

fn list_remove_batch<T>(list: &mut Vec<T>, index: &[i64]) -> Result<Vec<T>> {
    let mut elems = Vec::new();
    for i in index {
        let i = adjust_index_value(*i, list.len() + elems.len())?;
        let i = i
            .checked_sub(elems.len())
            .ok_or_else(|| Error::invalid_argument("index out of range"))?;
        elems.push(list.remove(i));
    }
    Ok(elems)
}

fn adjust_range_bounds(
    range: (Bound<i64>, Bound<i64>),
    len: usize,
) -> Result<(Bound<usize>, Bound<usize>)> {
    let start = adjust_range_bound(range.0, len)?;
    let end = adjust_range_bound(range.1, len)?;
    Ok((start, end))
}

fn adjust_range_bound(bound: Bound<i64>, len: usize) -> Result<Bound<usize>> {
    let len = i64::try_from(len).map_err(|_| Error::internal("convert usize to i64"))?;
    let bound = match bound {
        Bound::Included(v) => {
            let v = adjust_bound_value(v, len)?;
            Bound::Included(v)
        }
        Bound::Excluded(v) => {
            let v = adjust_bound_value(v, len)?;
            Bound::Excluded(v)
        }
        Bound::Unbounded => Bound::Unbounded,
    };
    Ok(bound)
}

fn adjust_bound_value(v: i64, len: i64) -> Result<usize> {
    let v = if v < 0 {
        if let Some(v) = v.checked_add(len) {
            if v < 0 {
                0
            } else {
                v
            }
        } else {
            return Err(Error::invalid_argument("range bound overflow"));
        }
    } else {
        v
    };
    usize::try_from(v).map_err(|_| Error::invalid_argument("convert i64 to usize"))
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
