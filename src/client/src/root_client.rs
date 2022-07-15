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

use derivative::Derivative;
use engula_api::{
    server::v1::*,
    v1::{create_collection_request::Partition, *},
};
use prost::{DecodeError, Message};
use tokio::sync::Mutex;
use tonic::{transport::Channel, Code, Status, Streaming};
use tracing::trace;

use crate::{conn_manager::ConnManager, discovery::ServiceDiscovery, NodeClient};

#[derive(thiserror::Error, Debug)]
enum RootError {
    #[error("not root")]
    NotRoot(RootDesc, Option<ReplicaDesc>),
    #[error("not available")]
    NotAvailable,
    #[error("rpc")]
    Rpc(#[from] Status),
}

#[derive(Debug, Clone)]
pub struct Client {
    // client: Arc<Mutex<root_client::RootClient<Channel>>>,
    shared: Arc<ClientShared>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientShared {
    #[derivative(Debug = "ignore")]
    discovery: Box<dyn ServiceDiscovery>,
    conn_manager: ConnManager,
    core: Mutex<ClientCore>,

    // Only one task is allowed to refresh root descriptor at a time.
    // The value is the latest epoch refreshed from nodes.
    refresh_descriptor_lock: Mutex<u64>,
}

#[derive(Debug, Clone)]
struct ClientCore {
    /// The id of node which serve leader replica.
    leader: Option<usize>,
    root: Arc<RootDesc>,
}

impl Client {
    pub fn new(discovery: Box<dyn ServiceDiscovery>, conn_manager: ConnManager) -> Self {
        Client {
            shared: Arc::new(ClientShared {
                discovery,
                conn_manager,
                core: Mutex::new(ClientCore {
                    leader: None,
                    root: Arc::default(),
                }),
                refresh_descriptor_lock: Mutex::new(0),
            }),
        }
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

    pub async fn alloc_replica(
        &self,
        req: AllocReplicaRequest,
    ) -> Result<AllocReplicaResponse, crate::Error> {
        let resp = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.alloc_replica(req).await }
            })
            .await?;
        Ok(resp.into_inner())
    }

    async fn invoke<F, O, V>(&self, op: F) -> Result<V, crate::Error>
    where
        F: Fn(root_client::RootClient<Channel>) -> O,
        O: Future<Output = Result<V, Status>>,
    {
        let mut core = self.core().await;
        'OUTER: loop {
            if let Some(leader) = core.leader {
                // do fast path.
                let leader_node = &core.root.root_nodes[leader];
                let client = self
                    .shared
                    .conn_manager
                    .get_root_client(leader_node.addr.clone())
                    .await?;
                match invoke(client, &op).await {
                    Ok(res) => return Ok(res),
                    Err(RootError::NotAvailable) => {
                        trace!(
                            "send rpc to root {}: remote is not available",
                            leader_node.addr
                        );
                    }
                    Err(RootError::NotRoot(root, leader_opt)) => {
                        if let Some(leader) = leader_opt {
                            // FIXME(walter) 也许 Not Leader 也返回 epoch 会更好？
                            let mut core = self.shared.core.lock().await;
                            if root.epoch > core.root.epoch {
                                core.root = Arc::new(root);
                                core.leader = Some(0); // TODO(walter) find the index of leader.
                            }
                        } else if core.root.epoch < root.epoch {
                            // root is updated, try find the leader of root.
                            core.leader = None;
                            core.root = Arc::new(root);
                        }

                        // Testing the specified root nodes.
                    }
                    Err(RootError::Rpc(status)) => return Err(status.into()),
                };
            }

            // Enter slow path
            for (i, node) in core.root.root_nodes.iter().enumerate() {
                if matches!(core.leader, Some(i)) {
                    continue;
                }

                let client = self
                    .shared
                    .conn_manager
                    .get_root_client(node.addr.clone())
                    .await?;
                match invoke(client, &op).await {
                    Ok(res) => {
                        // 1. save new leader of root.
                        core.leader = Some(i);
                        let mut core_guard = self.shared.core.lock().await;
                        if core_guard.root.epoch <= core.root.epoch {
                            // TODO(walter) add epoch so that we could found the accurate
                            // leader.
                            *core_guard = core;
                        }
                        return Ok(res);
                    }
                    Err(RootError::NotAvailable) => {
                        // Connect timeout or refused, try next address.
                    }
                    Err(RootError::NotRoot(root, leader_opt)) => {
                        // Some payloads exists, might is conditions.
                        if core.root.epoch < root.epoch {
                            // Find a freshed epoch.
                            core.leader = None; // TODO(walter) the leader_opt is useable.
                            core.root = Arc::new(root);
                            continue 'OUTER;
                        }
                    }
                    Err(RootError::Rpc(status)) => {
                        return Err(status.into());
                    }
                }
            }

            let refresh_guard = self.shared.refresh_descriptor_lock.lock().await;
            {
                let core_guard = self.shared.core.lock().await;
                if core_guard.root.epoch > core.root.epoch {
                    // already found, try next round.
                    continue;
                }
            }

            // Someone is refreshed.
            // Sine all nodes are unreachable or timeout, try refresh root from discovery?
            if let Some(root) = self.refresh_root_descriptor(core.root.epoch).await? {
                core.leader = None;
                core.root = Arc::new(root);
            }
        }
    }

    async fn refresh_root_descriptor(
        &self,
        local_epoch: u64,
    ) -> Result<Option<RootDesc>, crate::Error> {
        let nodes = self.shared.discovery.list_nodes().await;
        for node in nodes {
            let node_client = self
                .shared
                .conn_manager
                .get_node_client(node.clone())
                .await?;
            if let Ok(root) = node_client.get_root().await {
                if root.epoch > local_epoch {
                    return Ok(Some(root));
                }
            }
        }
        Ok(None)
    }

    #[inline]
    async fn core(&self) -> ClientCore {
        self.shared.core.lock().await.clone()
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

    pub fn create_collection(
        db_name: String,
        co_name: String,
        partition: Option<Partition>,
    ) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::CreateCollection(
                    CreateCollectionRequest {
                        name: co_name,
                        parent: db_name,
                        partition,
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

fn extract_root_descriptor(status: &tonic::Status) -> Option<(RootDesc, Option<ReplicaDesc>)> {
    use error_detail_union::Value;
    if status.code() == Code::Unknown && !status.details().is_empty() {
        if let Ok(err) = Error::decode(status.details()) {
            if !err.details.is_empty() {
                // Only convert first error detail.
                let detail = &err.details[0];
                let msg = detail.message.clone();
                if let Some(Value::NotRoot(not_root)) =
                    detail.detail.as_ref().and_then(|u| u.value.clone())
                {
                    return Some((not_root.root.unwrap_or_default(), not_root.leader));
                }
            }
        }
    }

    None
}

async fn invoke<F, O, V>(client: root_client::RootClient<Channel>, op: &F) -> Result<V, RootError>
where
    F: Fn(root_client::RootClient<Channel>) -> O,
    O: Future<Output = Result<V, Status>>,
{
    match op(client).await {
        Ok(res) => Ok(res),
        Err(status) => match status.code() {
            Code::Unavailable | Code::DeadlineExceeded => Err(RootError::NotAvailable),
            Code::Unknown if !status.details().is_empty() => {
                let (root, leader_opt) =
                    extract_root_descriptor(&status).ok_or::<RootError>(status.into())?;
                Err(RootError::NotRoot(root, leader_opt))
            }
            _ => Err(status.into()),
        },
    }
}
