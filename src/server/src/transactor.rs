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

use std::collections::HashMap;

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
        for sub_req in req.collections {
            // Assumes that all collections exist for now.
            let co = self
                .collections
                .entry(sub_req.name.clone())
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
        for method in req.methods {
            let id = method
                .index
                .and_then(|x| {
                    if let method_call_expr::Index::BlobIdent(id) = x {
                        Some(id)
                    } else {
                        None
                    }
                })
                .ok_or(Error::InvalidRequest)?;
            let call = method.call.ok_or(Error::InvalidRequest)?;
            let func = call.function.ok_or(Error::InvalidRequest)?;
            let mut args = call.arguments;
            let mut result = MethodCallResult::default();
            match func {
                call_expr::Function::Generic(f) => {
                    match GenericFunction::from_i32(f).ok_or(Error::InvalidRequest)? {
                        GenericFunction::Get => {
                            let value = self.objects.get(&id).cloned();
                            result.value = Some(value.unwrap_or_default());
                        }
                        GenericFunction::Set => {
                            if args.is_empty() {
                                return Err(Error::InvalidRequest);
                            }
                            self.objects.insert(id, args.swap_remove(0));
                        }
                    }
                }
                call_expr::Function::Numeric(f) => {
                    match NumericFunction::from_i32(f).ok_or(Error::InvalidRequest)? {
                        NumericFunction::Add => {
                            if args.is_empty() {
                                return Err(Error::InvalidRequest);
                            }
                            let operand = args.swap_remove(0);
                            if let Some(value) =
                                self.objects.get_mut(&id).and_then(|x| x.value.as_mut())
                            {
                                match value {
                                    generic_value::Value::Int64Value(x) => {
                                        match operand.value.ok_or(Error::InvalidRequest)? {
                                            generic_value::Value::Int64Value(v) => {
                                                *x += v;
                                            }
                                            _ => todo!(),
                                        }
                                    }
                                    _ => todo!(),
                                }
                            } else {
                                self.objects.insert(id, operand);
                            }
                        }
                    }
                }
                call_expr::Function::Container(f) => {
                    match ContainerFunction::from_i32(f).ok_or(Error::InvalidRequest)? {
                        ContainerFunction::Delete => {
                            self.objects.remove(&id);
                        }
                        _ => todo!(),
                    }
                }
            }
            res.results.push(result);
        }
        Ok(res)
    }
}
