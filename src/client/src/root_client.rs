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

use engula_api::{server::v1::*, v1::*};
use prost::{DecodeError, Message};
use tokio::sync::Mutex;
use tonic::{transport::Channel, Streaming};

use crate::NodeClient;

#[derive(Debug, Clone)]
pub struct Client {
    ensemble: Vec<String>,
    client: Arc<Mutex<root_client::RootClient<Channel>>>,
}

async fn find_root_client(
    ensemble: &[String],
) -> Result<root_client::RootClient<Channel>, crate::Error> {
    let node_client = find_node_client(ensemble).await?;
    let root_candidates = node_client.get_root().await?;
    let mut errs = vec![];
    for addr in root_candidates {
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

fn is_not_leader(err: Result<Error, DecodeError>) -> bool {
    if err.is_err() {
        return false;
    }

    let details = err.unwrap().details;
    if details.is_empty() {
        return false;
    }

    let v = &details[0].detail.as_ref().and_then(|u| u.value.clone());
    matches!(v, Some(error_detail_union::Value::NotLeader(_)))
}

impl Client {
    pub async fn connect(ensemble: Vec<String>) -> Result<Self, crate::Error> {
        let client = find_root_client(ensemble.as_slice()).await?;
        let client = Arc::new(Mutex::new(client));
        Ok(Self { ensemble, client })
    }

    pub async fn admin(&self, req: AdminRequest) -> Result<AdminResponse, crate::Error> {
        let mut client = self.client.lock().await;
        let res = match client.admin(req.clone()).await {
            Ok(res) => res,
            Err(status) => {
                if is_not_leader(Error::decode(status.details())) {
                    *client = find_root_client(self.ensemble.as_slice()).await?;
                    client.admin(req.clone()).await?
                } else {
                    return Err(status.into());
                }
            }
        };
        Ok(res.into_inner())
    }

    pub async fn join_node(&self, req: JoinNodeRequest) -> Result<JoinNodeResponse, crate::Error> {
        let mut client = self.client.lock().await;
        let res = match client.join(req.clone()).await {
            Ok(res) => res,
            Err(status) => {
                if is_not_leader(Error::decode(status.details())) {
                    *client = find_root_client(self.ensemble.as_slice()).await?;
                    client.join(req.clone()).await?
                } else {
                    return Err(status.into());
                }
            }
        };
        Ok(res.into_inner())
    }

    // TODO removed once `watch` implemented
    pub async fn resolve(&self, node_id: u64) -> Result<ResolveNodeResponse, crate::Error> {
        let req = ResolveNodeRequest { node_id };
        let mut client = self.client.lock().await;
        let res = match client.resolve(req.clone()).await {
            Ok(res) => res,
            Err(status) => {
                if is_not_leader(Error::decode(status.details())) {
                    *client = find_root_client(self.ensemble.as_slice()).await?;
                    client.resolve(req.clone()).await?
                } else {
                    return Err(status.into());
                }
            }
        };
        Ok(res.into_inner())
    }

    pub async fn watch(
        &self,
        cur_group_epochs: HashMap<u64, u64>,
    ) -> Result<Streaming<WatchResponse>, crate::Error> {
        let req = WatchRequest { cur_group_epochs };
        let mut client = self.client.lock().await;
        let res = match client.watch(req.clone()).await {
            Ok(res) => res,
            Err(status) => {
                if is_not_leader(Error::decode(status.details())) {
                    *client = find_root_client(self.ensemble.as_slice()).await?;
                    client.watch(req.clone()).await?
                } else {
                    return Err(status.into());
                }
            }
        };
        Ok(res.into_inner())
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
