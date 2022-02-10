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

use std::collections::{hash_map::Entry, HashMap};

use engula_apis::*;
use tokio::sync::Mutex;

use crate::{Error, Result};

pub struct Cooperator {
    databases: Mutex<HashMap<u64, Database>>,
}

impl Default for Cooperator {
    fn default() -> Self {
        Self::new()
    }
}

impl Cooperator {
    pub fn new() -> Self {
        Self {
            databases: Mutex::new(HashMap::new()),
        }
    }

    pub async fn execute(&self, req: TxnRequest) -> Result<TxnResponse> {
        let mut databases = self.databases.lock().await;
        let mut res = TxnResponse::default();
        for sub_req in req.requests {
            // Assumes that all databases exist for now.
            let db = databases
                .entry(sub_req.database_id)
                .or_insert_with(Database::new);
            let sub_res = db.execute(sub_req)?;
            res.responses.push(sub_res);
        }
        Ok(res)
    }
}

struct Database {
    collections: HashMap<u64, Collection>,
}

impl Database {
    fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    fn execute(&mut self, req: DatabaseTxnRequest) -> Result<DatabaseTxnResponse> {
        let mut res = DatabaseTxnResponse::default();
        for sub_req in req.collections {
            // Assumes that all collections exist for now.
            let co = self
                .collections
                .entry(sub_req.collection_id)
                .or_insert_with(Collection::new);
            let sub_res = co.execute(sub_req)?;
            res.collections.push(sub_res);
        }
        Ok(res)
    }
}

struct Collection {
    objects: HashMap<Vec<u8>, GenericValue>,
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
            let id = expr.id;
            let call = expr.call.ok_or(Error::InvalidRequest)?;
            let func = call.function.ok_or(Error::InvalidRequest)?;
            let mut args = call.arguments;
            match func {
                call_expr::Function::Generic(f) => match GenericFunction::from_i32(f) {
                    Some(GenericFunction::Get) => {
                        let value = self.objects.get(&id).cloned().unwrap_or_default();
                        res.values.push(value);
                    }
                    Some(GenericFunction::Set) => {
                        if args.is_empty() {
                            return Err(Error::InvalidRequest);
                        }
                        self.objects.insert(id, args.swap_remove(0));
                    }
                    Some(GenericFunction::Delete) => {
                        self.objects.remove(&id);
                    }
                    None => return Err(Error::InvalidRequest),
                },
                call_expr::Function::Numeric(f) => match NumericFunction::from_i32(f) {
                    Some(NumericFunction::Add) => {
                        if args.is_empty() {
                            return Err(Error::InvalidRequest);
                        }
                        let operand = args.swap_remove(0);
                        match self.objects.entry(id) {
                            Entry::Vacant(ent) => {
                                ent.insert(operand);
                            }
                            Entry::Occupied(mut ent) => match &mut ent.get_mut().value {
                                Some(generic_value::Value::Int64Value(x)) => {
                                    match operand.value.ok_or(Error::InvalidRequest)? {
                                        generic_value::Value::Int64Value(v) => {
                                            *x += v;
                                        }
                                        _ => todo!(),
                                    }
                                }
                                None => return Err(Error::InvalidRequest),
                                _ => todo!(),
                            },
                        }
                    }
                    None => return Err(Error::InvalidRequest),
                },
            }
        }
        Ok(res)
    }
}
