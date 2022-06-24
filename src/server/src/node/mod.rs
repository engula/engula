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

pub mod engine;
mod job;
pub mod migrate;
pub mod replica;
pub mod resolver;
pub mod route_table;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use engula_api::server::v1::*;
use futures::lock::Mutex;
use tracing::{debug, info, warn};

pub use self::{
    engine::{GroupEngine, StateEngine},
    replica::Replica,
    route_table::{RaftRouteTable, ReplicaRouteTable},
};
use self::{
    job::StateChannel,
    migrate::{MigrateController, ShardChunkStream},
};
use crate::{
    node::replica::{ExecCtx, ReplicaInfo},
    raftgroup::{AddressResolver, RaftManager, TransportManager},
    runtime::{Executor, JoinHandle},
    serverpb::v1::*,
    Error, Result,
};

struct ReplicaContext {
    info: Arc<ReplicaInfo>,
    tasks: Vec<JoinHandle<()>>,
}

/// A structure holds the states of node. Eg create replica.
#[derive(Default)]
struct NodeState
where
    Self: Send,
{
    ident: Option<NodeIdent>,
    replicas: HashMap<u64, ReplicaContext>,
    root: Vec<NodeDesc>,
    channel: Option<StateChannel>,
}

#[derive(Clone)]
pub struct Node
where
    Self: Send + Sync,
{
    raw_db: Arc<rocksdb::DB>,
    executor: Executor,
    state_engine: StateEngine,
    raft_route_table: RaftRouteTable,
    replica_route_table: ReplicaRouteTable,

    raft_mgr: RaftManager,
    migrate_ctrl: MigrateController,

    /// `NodeState` of this node, the lock is used to ensure serialization of create/terminate
    /// replica operations.
    node_state: Arc<Mutex<NodeState>>,
}

impl Node {
    pub fn new(
        log_path: PathBuf,
        raw_db: Arc<rocksdb::DB>,
        state_engine: StateEngine,
        executor: Executor,
        address_resolver: Arc<dyn AddressResolver>,
    ) -> Result<Self> {
        let raft_route_table = RaftRouteTable::new();
        let trans_mgr = TransportManager::build(
            executor.clone(),
            address_resolver.clone(),
            raft_route_table.clone(),
        );
        let raft_mgr = RaftManager::open(log_path, executor.clone(), trans_mgr)?;
        let migrate_ctrl = MigrateController::new(address_resolver, executor.clone());
        Ok(Node {
            raw_db,
            executor,
            state_engine,
            raft_route_table,
            replica_route_table: ReplicaRouteTable::new(),
            raft_mgr,
            migrate_ctrl,
            node_state: Arc::new(Mutex::new(NodeState::default())),
        })
    }

    /// Bootstrap node and recover alive replicas.
    pub async fn bootstrap(&self, node_ident: &NodeIdent) -> Result<()> {
        let mut node_state = self.node_state.lock().await;
        debug_assert!(
            node_state.replicas.is_empty(),
            "some replicas are serving before recovery?"
        );

        node_state.ident = Some(node_ident.to_owned());
        node_state.channel = Some(self::job::setup_report_state(
            &self.executor,
            self.state_engine.clone(),
        ));

        let node_id = node_ident.node_id;
        let it = self.state_engine.iterate_replica_states().await;
        for (group_id, replica_id, state) in it {
            match state {
                ReplicaLocalState::Tombstone => continue,
                ReplicaLocalState::Terminated => {
                    self::job::setup_destory_replica(
                        &self.executor,
                        group_id,
                        replica_id,
                        self.state_engine.clone(),
                        self.raw_db.clone(),
                    );
                    continue;
                }
                _ => {}
            };

            let desc = ReplicaDesc {
                id: replica_id,
                node_id,
                ..Default::default()
            };
            let context = self
                .serve_replica(
                    group_id,
                    desc,
                    state,
                    node_state.channel.as_ref().unwrap().clone(),
                )
                .await?;
            node_state.replicas.insert(replica_id, context);
        }

        Ok(())
    }

    /// Create a replica. If this node has been bootstrapped, start the replica.
    ///
    /// The replica state is determined by the `GroupDesc`.
    ///
    /// NOTE: This function is idempotent.
    pub async fn create_replica(&self, replica_id: u64, group: GroupDesc) -> Result<()> {
        let mut node_state = self.node_state.lock().await;
        if node_state.replicas.contains_key(&replica_id) {
            debug!(replica = replica_id, "replica already exists");
            return Ok(());
        }

        // To ensure crash-recovery consistency, first create raft metadata, and then create group
        // metadata. In this way, even if the group is restarted before the group is successfully
        // created, a replica can be recreated by retrying.
        let group_id = group.id;
        Replica::create(replica_id, &group, &self.raft_mgr).await?;
        GroupEngine::create(self.raw_db.clone(), &group).await?;
        let replica_state = if group.replicas.is_empty() {
            ReplicaLocalState::Pending
        } else {
            ReplicaLocalState::Initial
        };
        self.state_engine
            .save_replica_state(group_id, replica_id, replica_state)
            .await?;

        info!(
            "create replica {} of group {} and write initial state success",
            replica_id, group_id
        );

        // If this node has not completed initialization, then there is no need to record
        // `ReplicaInfo`. Because the recovery operation will be performed later, `ReplicaMeta` will
        // be read again and the corresponding `ReplicaInfo` will be created.
        if node_state.is_bootstrapped() {
            let node_id = node_state.ident.as_ref().unwrap().node_id;
            let desc = ReplicaDesc {
                id: replica_id,
                node_id,
                ..Default::default()
            };
            let context = self
                .serve_replica(
                    group_id,
                    desc,
                    replica_state,
                    node_state.channel.as_ref().unwrap().clone(),
                )
                .await?;
            node_state.replicas.insert(replica_id, context);
        }

        Ok(())
    }

    /// Remove the specified replica.
    #[allow(unused)]
    pub async fn remove_replica(&self, replica_id: u64, actual_desc: &GroupDesc) -> Result<()> {
        let group_id = actual_desc.id;
        let replica = match self.replica_route_table.find(group_id) {
            Some(replica) => replica,
            None => {
                warn!(
                    group_id = group_id,
                    replica_id = replica_id,
                    "remove a not existed replica"
                );
                return Ok(());
            }
        };

        replica.shutdown(actual_desc).await?;
        self.replica_route_table.remove(group_id);

        let tasks = {
            let mut node_state = self.node_state.lock().await;
            let ctx = node_state
                .replicas
                .remove(&replica_id)
                .expect("replica should exists");
            ctx.tasks
        };

        for handle in tasks {
            handle.await;
        }

        // This replica is shutdowned, we need to update and persisted states.
        self.state_engine
            .save_replica_state(group_id, replica_id, ReplicaLocalState::Terminated)
            .await?;

        // Clean group engine data in asynchronously.
        self::job::setup_destory_replica(
            &self.executor,
            group_id,
            replica_id,
            self.state_engine.clone(),
            self.raw_db.clone(),
        );

        Ok(())
    }

    /// Open, recover replica and start serving.
    async fn serve_replica(
        &self,
        group_id: u64,
        desc: ReplicaDesc,
        local_state: ReplicaLocalState,
        channel: StateChannel,
    ) -> Result<ReplicaContext> {
        use self::replica::job;

        let group_engine = match GroupEngine::open(group_id, self.raw_db.clone()).await? {
            Some(group_engine) => group_engine,
            None => {
                panic!(
                    "no such group engine exists, group {}, replica {}",
                    group_id, desc.id
                );
            }
        };

        let replica_id = desc.id;
        let replica = Replica::recover(
            group_id,
            desc,
            local_state,
            channel,
            group_engine,
            &self.raft_mgr,
            self.migrate_ctrl.clone(),
        )
        .await?;
        let replica = Arc::new(replica);
        self.replica_route_table.update(replica.clone());
        self.raft_route_table
            .update(replica_id, replica.raft_node());

        let handles = vec![job::setup_purge_replica(
            self.executor.clone(),
            replica.clone(),
        )];

        Ok(ReplicaContext {
            info: replica.replica_info(),
            tasks: handles,
        })
    }

    /// Get root addrs that known by node.
    pub async fn get_root(&self) -> Vec<String> {
        let nodes = self.node_state.lock().await.root.to_owned();
        nodes.iter().map(|n| n.addr.to_owned()).collect()
    }

    // Update recent known root nodes.
    pub async fn update_root(&self, roots: Vec<NodeDesc>) -> Result<()> {
        self.state_engine().save_root_nodes(roots).await?;
        self.reload_root_from_engine().await
    }

    pub async fn reload_root_from_engine(&self) -> Result<()> {
        let nodes = self
            .state_engine()
            .load_root_nodes()
            .await?
            .ok_or_else(|| Error::InvalidData("root not found".into()))?;
        self.node_state.lock().await.root = nodes;
        Ok(())
    }

    pub async fn execute_request(&self, request: GroupRequest) -> Result<GroupResponse> {
        use self::replica::retry::execute;

        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };

        execute(&replica, ExecCtx::default(), request).await
    }

    pub async fn pull_shard_chunks(&self, request: PullRequest) -> Result<ShardChunkStream> {
        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };
        Ok(ShardChunkStream::new(
            request.shard_id,
            request.last_key,
            replica,
        ))
    }

    pub async fn forward(&self, request: ForwardRequest) -> Result<ForwardResponse> {
        use self::replica::retry::execute;

        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };

        let ingest_chunk = ShardChunk {
            data: request.forward_data,
        };
        replica.ingest(request.shard_id, ingest_chunk, true).await?;

        debug_assert!(request.request.is_some());
        let group_request = GroupRequest {
            group_id: request.group_id,
            epoch: 0,
            request: request.request,
        };

        let exec_ctx = ExecCtx::forward(request.shard_id);
        let resp = execute(&replica, exec_ctx, group_request).await?;
        debug_assert!(resp.response.is_some());
        Ok(ForwardResponse {
            response: resp.response,
        })
    }

    #[inline]
    pub fn replica_table(&self) -> &ReplicaRouteTable {
        &self.replica_route_table
    }

    #[inline]
    pub fn raft_route_table(&self) -> &RaftRouteTable {
        &self.raft_route_table
    }

    #[inline]
    pub fn state_engine(&self) -> &StateEngine {
        &self.state_engine
    }

    #[inline]
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    #[inline]
    pub fn raft_manager(&self) -> &RaftManager {
        &self.raft_mgr
    }

    pub async fn collect_stats(&self, _req: &CollectStatsRequest) -> CollectStatsResponse {
        // TODO(walter) add read/write qps.
        let mut ns = NodeStats::default();
        let mut group_stats = vec![];
        let mut replica_stats = vec![];
        let group_id_list = self.serving_group_id_list().await;
        for group_id in group_id_list {
            if let Some(replica) = self.replica_route_table.find(group_id) {
                let info = replica.replica_info();
                if info.is_terminated() {
                    continue;
                }

                ns.group_count += 1;
                let descriptor = replica.descriptor();
                if descriptor.replicas.is_empty() {
                    ns.orphan_replica_count += 1;
                }
                let replica_state = replica.replica_state();
                if replica_state.role == RaftRole::Leader.into() {
                    ns.leader_count += 1;
                    let gs = GroupStats {
                        group_id: info.group_id,
                        shard_count: descriptor.shards.len() as u64,
                        read_qps: 0.,
                        write_qps: 0.,
                    };
                    group_stats.push(gs);
                }
                let rs = ReplicaStats {
                    replica_id: info.replica_id,
                    group_id: info.group_id,
                    read_qps: 0.,
                    write_qps: 0.,
                };
                replica_stats.push(rs);
            }
        }

        CollectStatsResponse {
            node_stats: Some(ns),
            group_stats,
            replica_stats,
        }
    }

    pub async fn collect_group_detail(
        &self,
        req: &CollectGroupDetailRequest,
    ) -> CollectGroupDetailResponse {
        let mut group_id_list = req.groups.clone();
        if group_id_list.is_empty() {
            group_id_list = self.serving_group_id_list().await;
        }

        let mut states = vec![];
        let mut descriptors = vec![];
        for group_id in group_id_list {
            if let Some(replica) = self.replica_route_table.find(group_id) {
                if replica.replica_info().is_terminated() {
                    continue;
                }

                let state = replica.replica_state();
                if state.role == RaftRole::Leader.into() {
                    descriptors.push(replica.descriptor());
                }
                states.push(state);
            }
        }

        CollectGroupDetailResponse {
            replica_states: states,
            group_descs: descriptors,
        }
    }

    #[inline]
    async fn serving_group_id_list(&self) -> Vec<u64> {
        let node_state = self.node_state.lock().await;
        node_state
            .replicas
            .iter()
            .map(|(_, ctx)| ctx.info.group_id)
            .collect()
    }
}

impl NodeState {
    #[inline]
    fn is_bootstrapped(&self) -> bool {
        self.ident.is_some()
    }
}

#[cfg(test)]
mod tests {
    use engula_api::server::v1::{ReplicaDesc, ReplicaRole};
    use tempdir::TempDir;

    use super::*;
    use crate::{bootstrap::INITIAL_EPOCH, runtime::ExecutorOwner};

    fn create_node(executor: Executor) -> Node {
        let tmp_dir = TempDir::new("engula").unwrap().into_path();
        let db_dir = tmp_dir.join("db");
        let log_dir = tmp_dir.join("log");

        use crate::bootstrap::open_engine;

        let db = open_engine(db_dir).unwrap();
        let db = Arc::new(db);
        let state_engine = StateEngine::new(db.clone()).unwrap();
        let address_resolver = Arc::new(crate::node::resolver::AddressResolver::new(vec![]));
        Node::new(log_dir, db, state_engine, executor, address_resolver).unwrap()
    }

    async fn replica_state(node: Node, replica_id: u64) -> Option<ReplicaLocalState> {
        node.state_engine()
            .iterate_replica_states()
            .await
            .filter(|(_, id, _)| *id == replica_id)
            .map(|(_, _, state)| state)
            .next()
    }

    #[test]
    fn create_pending_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let node = create_node(executor.clone());

        let group_id = 2;
        let replica_id = 2;
        let group = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas: vec![],
        };

        executor.block_on(async {
            node.create_replica(replica_id, group).await.unwrap();

            assert!(matches!(
                replica_state(node, replica_id).await,
                Some(ReplicaLocalState::Pending),
            ));
        });
    }

    #[test]
    fn create_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let node = create_node(executor.clone());

        let group_id = 2;
        let replica_id = 2;
        let group = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas: vec![ReplicaDesc {
                id: replica_id,
                node_id: 1,
                role: ReplicaRole::Voter.into(),
            }],
        };

        executor.block_on(async {
            node.create_replica(replica_id, group).await.unwrap();

            assert!(matches!(
                replica_state(node, replica_id).await,
                Some(ReplicaLocalState::Initial),
            ));
        });
    }

    #[test]
    fn recover() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let node = create_node(executor.clone());

        let group_id = 2;
        let replica_id = 2;
        let group = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas: vec![],
        };

        executor.block_on(async {
            node.create_replica(replica_id, group).await.unwrap();
        });

        drop(node);
        let node = create_node(executor.clone());
        executor.block_on(async {
            let ident = NodeIdent {
                cluster_id: vec![],
                node_id: 1,
            };
            node.bootstrap(&ident).await.unwrap();
        })
    }
}
