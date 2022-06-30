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

use std::{collections::HashMap, future::Future, sync::Arc};

use engula_api::{server::v1::*, v1::*};
use prost::{DecodeError, Message};
use tokio::sync::Mutex;
use tonic::{transport::Channel, Status, Streaming};

use crate::NodeClient;

#[derive(Debug, Clone)]
pub struct Client {
    client: Arc<Mutex<root_client::RootClient<Channel>>>,
}

async fn find_root_client(
    candidates: &[String],
) -> Result<root_client::RootClient<Channel>, crate::Error> {
    let mut errs = vec![];
    for addr in candidates {
        let root_addr = format!("http://{}", addr);
        match root_client::RootClient::connect(root_addr).await {
            Ok(client) => return Ok(client),
            Err(err) => errs.push(err),
        }
    }
    Err(crate::Error::MultiTransport(errs))
}

async fn find_node_client(ensemble: &[String]) -> Result<NodeClient, crate::Error> {
    let mut errs = vec![];
    for addr in ensemble {
        match NodeClient::connect(addr.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => errs.push(err),
        }
    }
    Err(crate::Error::MultiTransport(errs))
}

fn not_root(err: Result<Error, DecodeError>) -> Option<NotRoot> {
    let details = err.ok().map(|e| e.details)?;

    if details.is_empty() {
        return None;
    }

    match &details[0].detail.as_ref().and_then(|u| u.value.clone()) {
        Some(error_detail_union::Value::NotRoot(res)) => Some(res.clone()),
        _ => None,
    }
}

impl Client {
    pub async fn connect(ensemble: Vec<String>) -> Result<Self, crate::Error> {
        let node_client = find_node_client(ensemble.as_slice()).await?;
        let candidates = node_client.get_root().await?;
        let root_client = find_root_client(candidates.as_slice()).await?;
        let client = Arc::new(Mutex::new(root_client));
        Ok(Self { client })
    }

    pub async fn admin(&self, req: AdminRequest) -> Result<AdminResponse, crate::Error> {
        let res = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.admin(req).await }
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn join_node(&self, req: JoinNodeRequest) -> Result<JoinNodeResponse, crate::Error> {
        let res = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.join(req).await }
            })
            .await?;
        Ok(res.into_inner())
    }

    // TODO removed once `watch` implemented
    pub async fn resolve(&self, node_id: u64) -> Result<ResolveNodeResponse, crate::Error> {
        let req = ResolveNodeRequest { node_id };
        let res = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.resolve(req).await }
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn watch(
        &self,
        cur_group_epochs: HashMap<u64, u64>,
    ) -> Result<Streaming<WatchResponse>, crate::Error> {
        let req = WatchRequest { cur_group_epochs };
        let res = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.watch(req).await }
            })
            .await?;
        Ok(res.into_inner())
    }

    async fn invoke<F, O, V>(&self, op: F) -> Result<V, crate::Error>
    where
        F: Fn(root_client::RootClient<Channel>) -> O,
        O: Future<Output = Result<V, Status>>,
    {
        let mut client = self.client.lock().await;
        match op(client.clone()).await {
            Ok(res) => Ok(res),
            Err(status) => {
                let err = not_root(Error::decode(status.details())).ok_or(status)?;
                let candidates = err.root;
                *client = find_root_client(candidates.as_slice()).await?;
                Ok(op(client.clone()).await?)
            }
        }
    }
}

pub struct AdminRequestBuilder;

impl AdminRequestBuilder {
    pub fn create_database(name: String) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::CreateDatabase(
                    CreateDatabaseRequest { name },
                )),
            }),
        }
    }

    pub fn get_database(name: String) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::GetDatabase(
                    GetDatabaseRequest { name },
                )),
            }),
        }
    }

    pub fn create_collection(db_name: String, co_name: String) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::CreateCollection(
                    CreateCollectionRequest {
                        name: co_name,
                        parent: db_name,
                    },
                )),
            }),
        }
    }

    pub fn get_collection(db_name: String, co_name: String) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::GetCollection(
                    GetCollectionRequest {
                        name: co_name,
                        parent: db_name,
                    },
                )),
            }),
        }
    }
}

pub struct AdminResponseExtractor;

impl AdminResponseExtractor {
    pub fn create_database(resp: AdminResponse) -> Option<DatabaseDesc> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::CreateDatabase(response)),
        }) = resp.response
        {
            response.database
        } else {
            None
        }
    }

    pub fn get_database(resp: AdminResponse) -> Option<DatabaseDesc> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::GetDatabase(response)),
        }) = resp.response
        {
            response.database
        } else {
            None
        }
    }

    pub fn create_collection(resp: AdminResponse) -> Option<CollectionDesc> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::CreateCollection(response)),
        }) = resp.response
        {
            response.collection
        } else {
            None
        }
    }

    pub fn get_collection(resp: AdminResponse) -> Option<CollectionDesc> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::GetCollection(response)),
        }) = resp.response
        {
            response.collection
        } else {
            None
        }
    }
}
