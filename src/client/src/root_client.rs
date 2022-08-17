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

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use derivative::Derivative;
use engula_api::{
    server::v1::{root_client::RootClient, *},
    v1::{create_collection_request::Partition, *},
};
use prost::Message;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Code, Status, Streaming};
use tracing::trace;

use crate::{
    conn_manager::ConnManager, discovery::ServiceDiscovery, error::retryable_rpc_err, NodeClient,
    Result,
};

#[derive(thiserror::Error, Debug)]
enum RootError {
    #[error("not root")]
    NotRoot(RootDesc, u64, Option<ReplicaDesc>),
    #[error("not available")]
    NotAvailable,
    #[error("rpc")]
    Rpc(#[from] Status),
}

pub struct AdminRequestBuilder;
pub struct AdminResponseExtractor;

#[derive(Debug, Clone)]
pub struct Client {
    shared: Arc<ClientShared>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientShared {
    #[derivative(Debug = "ignore")]
    discovery: Arc<dyn ServiceDiscovery>,
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
    term: u64,
    root: Arc<RootDesc>,
}

impl Client {
    pub fn new(discovery: Arc<dyn ServiceDiscovery>, conn_manager: ConnManager) -> Self {
        Client {
            shared: Arc::new(ClientShared {
                discovery,
                conn_manager,
                core: Mutex::new(ClientCore {
                    leader: None,
                    term: 0,
                    root: Arc::default(),
                }),
                refresh_descriptor_lock: Mutex::new(0),
            }),
        }
    }

    pub async fn report(&self, req: &ReportRequest) -> Result<ReportResponse> {
        let res = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.report(req).await }
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn admin(&self, req: AdminRequest) -> Result<AdminResponse> {
        let res = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.admin(req).await }
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn join_node(&self, req: JoinNodeRequest) -> Result<JoinNodeResponse> {
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
    ) -> Result<Streaming<WatchResponse>> {
        let req = WatchRequest { cur_group_epochs };
        let res = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.watch(req).await }
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn alloc_replica(&self, req: AllocReplicaRequest) -> Result<AllocReplicaResponse> {
        let resp = self
            .invoke(|mut client| {
                let req = req.clone();
                async move { client.alloc_replica(req).await }
            })
            .await?;
        Ok(resp.into_inner())
    }

    async fn invoke<F, O, V>(&self, op: F) -> Result<V>
    where
        F: Fn(root_client::RootClient<Channel>) -> O,
        O: Future<Output = std::result::Result<V, Status>>,
    {
        let mut interval = 1;
        let mut save_core = false;
        let mut core = self.core().await;
        'OUTER: loop {
            if let Some(leader) = core.leader {
                // Fast path of invoking.
                let leader_node = &core.root.root_nodes[leader];
                let client = self.get_root_client(leader_node.addr.clone()).await?;
                match invoke(client, &op).await {
                    Ok(res) => {
                        if save_core {
                            self.apply_core(core).await;
                        }
                        return Ok(res);
                    }
                    Err(RootError::Rpc(status)) => return Err(status.into()),
                    Err(RootError::NotAvailable) => {
                        trace!(
                            "send rpc to root {}: remote is not available",
                            leader_node.addr
                        );
                    }
                    Err(RootError::NotRoot(root, term, leader_opt)) => {
                        if core.root.epoch <= root.epoch {
                            // A new round is found, retry next times.
                            core.leader = None;
                            core.root = Arc::new(root);
                            if let Some(leader) = leader_opt {
                                if core.term < term {
                                    // Since leader exists, we don't need to iterate root nodes.
                                    core.apply_leader(leader, term);
                                    save_core = true;
                                    continue 'OUTER;
                                }
                            }
                        }
                    }
                };
            }

            // Slow path of invoking.
            for (i, node) in core.root.root_nodes.iter().enumerate() {
                if matches!(core.leader, Some(x) if x == i) {
                    continue;
                }

                let client = self.get_root_client(node.addr.clone()).await?;
                match invoke(client, &op).await {
                    Ok(res) => {
                        // Save new leader of root.
                        core.leader = Some(i);
                        self.apply_core(core).await;
                        return Ok(res);
                    }
                    Err(RootError::Rpc(status)) => {
                        return Err(status.into());
                    }
                    Err(RootError::NotAvailable) => {
                        // Connect timeout or refused, try next address.
                    }
                    Err(RootError::NotRoot(root, term, leader_opt)) => {
                        if core.root.epoch < root.epoch {
                            // A new root desc is found, iterate the new root nodes.
                            core.leader = None;
                            core.root = Arc::new(root);
                            if let Some(leader) = leader_opt {
                                if core.term < term {
                                    core.apply_leader(leader, term);
                                    save_core = true;
                                }
                            }
                            continue 'OUTER;
                        }
                    }
                }
            }

            // Sine all nodes are unreachable or timeout, try refresh roots from discovery.
            core = self.refresh_client_core(core).await?;

            tokio::time::sleep(Duration::from_millis(interval)).await;
            interval = std::cmp::min(interval * 2, 1000);
        }
    }

    #[inline]
    async fn core(&self) -> ClientCore {
        self.shared.core.lock().await.clone()
    }

    async fn apply_core(&self, core: ClientCore) {
        let mut core_guard = self.shared.core.lock().await;
        if core_guard.root.epoch <= core.root.epoch {
            // TODO(walter) add term so that we could found the accurate
            // leader.
            *core_guard = core;
        }
    }

    async fn refresh_root_descriptor(&self, local_epoch: u64) -> Result<Option<RootDesc>> {
        let nodes = self.shared.discovery.list_nodes().await;
        for node in nodes {
            let node_client = self.get_node_client(node).await?;
            if let Ok(root) = node_client.get_root().await {
                if root.epoch > local_epoch {
                    return Ok(Some(root));
                }
            }
        }
        Ok(None)
    }

    async fn refresh_client_core(&self, mut core: ClientCore) -> Result<ClientCore> {
        let _refresh_guard = self.shared.refresh_descriptor_lock.lock().await;
        {
            let core_guard = self.shared.core.lock().await;
            if core_guard.root.epoch > core.root.epoch {
                // already found, try next round.
                return Ok(core_guard.clone());
            }
        }

        if let Some(root) = self.refresh_root_descriptor(core.root.epoch).await? {
            // Someone is refreshed.
            core.leader = None;
            core.root = Arc::new(root);
        }
        Ok(core)
    }

    #[inline]
    async fn get_root_client(&self, addr: String) -> Result<RootClient<Channel>> {
        let root_client = self.shared.conn_manager.get_root_client(addr).await?;
        Ok(root_client)
    }

    #[inline]
    async fn get_node_client(&self, addr: String) -> Result<NodeClient> {
        let node_client = self.shared.conn_manager.get_node_client(addr).await?;
        Ok(node_client)
    }
}

impl ClientCore {
    fn apply_leader(&mut self, leader: ReplicaDesc, term: u64) {
        for (idx, node) in self.root.root_nodes.iter().enumerate() {
            if node.id == leader.node_id {
                self.leader = Some(idx);
                self.term = term;
                break;
            }
        }
    }
}

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

    pub fn delete_database(name: String) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::DeleteDatabase(
                    DeleteDatabaseRequest { name },
                )),
            }),
        }
    }

    pub fn list_database() -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::ListDatabases(
                    ListDatabasesRequest {},
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

    pub fn delete_collection(db_name: String, co_name: String) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::DeleteCollection(
                    DeleteCollectionRequest {
                        name: co_name,
                        parent: db_name,
                    },
                )),
            }),
        }
    }

    pub fn list_collection(parent: String) -> AdminRequest {
        AdminRequest {
            request: Some(AdminRequestUnion {
                request: Some(admin_request_union::Request::ListCollections(
                    ListCollectionsRequest { parent },
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

    pub fn delete_database(resp: AdminResponse) -> Option<()> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::DeleteDatabase(_)),
        }) = resp.response
        {
            Some(())
        } else {
            None
        }
    }

    pub fn list_database(resp: AdminResponse) -> Vec<DatabaseDesc> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::ListDatabases(response)),
        }) = resp.response
        {
            response.databases
        } else {
            vec![]
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

    pub fn delete_collection(resp: AdminResponse) -> Option<()> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::DeleteCollection(_)),
        }) = resp.response
        {
            Some(())
        } else {
            None
        }
    }

    pub fn list_collection(resp: AdminResponse) -> Vec<CollectionDesc> {
        if let Some(AdminResponseUnion {
            response: Some(admin_response_union::Response::ListCollections(response)),
        }) = resp.response
        {
            response.collections
        } else {
            vec![]
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

fn extract_root_descriptor(status: &tonic::Status) -> Option<(RootDesc, u64, Option<ReplicaDesc>)> {
    use error_detail_union::Value;
    if status.code() == Code::Unknown && !status.details().is_empty() {
        if let Ok(err) = Error::decode(status.details()) {
            if !err.details.is_empty() {
                // Only convert first error detail.
                let detail = &err.details[0];
                if let Some(Value::NotRoot(not_root)) =
                    detail.detail.as_ref().and_then(|u| u.value.clone())
                {
                    return Some((
                        not_root.root.unwrap_or_default(),
                        not_root.term,
                        not_root.leader,
                    ));
                }
            }
        }
    }

    None
}

async fn invoke<F, O, V>(
    client: root_client::RootClient<Channel>,
    op: &F,
) -> std::result::Result<V, RootError>
where
    F: Fn(root_client::RootClient<Channel>) -> O,
    O: Future<Output = std::result::Result<V, Status>>,
{
    match op(client).await {
        Ok(res) => Ok(res),
        Err(status) => match status.code() {
            Code::Unavailable | Code::DeadlineExceeded => Err(RootError::NotAvailable),
            Code::Unknown => {
                if status.details().is_empty() {
                    if retryable_rpc_err(&status) {
                        Err(RootError::NotAvailable)
                    } else {
                        Err(status.into())
                    }
                } else {
                    let (root, term, leader_opt) = extract_root_descriptor(&status)
                        .ok_or_else(|| <Status as Into<RootError>>::into(status))?;
                    Err(RootError::NotRoot(root, term, leader_opt))
                }
            }
            _ => Err(status.into()),
        },
    }
}
