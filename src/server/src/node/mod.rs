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
mod metrics;
pub mod migrate;
pub mod replica;
pub mod resolver;
pub mod route_table;

use std::{collections::HashMap, sync::Arc, time::Duration};

use engula_api::server::v1::*;
use futures::{channel::mpsc, lock::Mutex};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

pub use self::{
    engine::{GroupEngine, StateEngine},
    replica::Replica,
    route_table::{RaftRouteTable, ReplicaRouteTable},
};
use self::{
    job::StateChannel,
    migrate::{MigrateController, ShardChunkStream},
    replica::ReplicaConfig,
};
use crate::{
    bootstrap::ROOT_GROUP_ID,
    node::replica::{fsm::GroupStateMachine, ExecCtx, LeaseState, LeaseStateObserver, ReplicaInfo},
    raftgroup::{RaftManager, RaftNodeFacade, TransportManager},
    runtime::{sync::WaitGroup, Executor},
    serverpb::v1::*,
    Config, Error, Provider, Result,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeConfig {
    /// The limit bytes of each shard chunk during migration.
    ///
    /// Default: 64KB.
    pub shard_chunk_size: usize,

    /// The limit number of keys for gc shard after migration.
    ///
    /// Default: 256.
    pub shard_gc_keys: usize,

    pub replica: ReplicaConfig,
}

struct ReplicaContext {
    info: Arc<ReplicaInfo>,
    wait_group: WaitGroup,
}

/// A structure holds the states of node. Eg create replica.
#[derive(Default)]
struct NodeState
where
    Self: Send,
{
    ident: Option<NodeIdent>,
    replicas: HashMap<u64, ReplicaContext>,
    root: RootDesc,
    channel: Option<StateChannel>,
}

#[derive(Clone)]
pub struct Node
where
    Self: Send + Sync,
{
    cfg: NodeConfig,
    provider: Arc<Provider>,
    raft_route_table: RaftRouteTable,
    replica_route_table: ReplicaRouteTable,

    raft_mgr: RaftManager,
    migrate_ctrl: MigrateController,

    /// `NodeState` of this node, the lock is used to ensure serialization of create/terminate
    /// replica operations.
    node_state: Arc<Mutex<NodeState>>,
}

impl Node {
    pub(crate) fn new(cfg: Config, provider: Arc<Provider>) -> Result<Self> {
        let raft_route_table = RaftRouteTable::new();
        let trans_mgr = TransportManager::build(
            provider.executor.clone(),
            provider.address_resolver.clone(),
            raft_route_table.clone(),
        );
        let raft_mgr = RaftManager::open(cfg.raft.clone(), provider.clone(), trans_mgr)?;
        let migrate_ctrl = MigrateController::new(cfg.node.clone(), provider.clone());
        Ok(Node {
            cfg: cfg.node,
            provider,
            raft_route_table,
            replica_route_table: ReplicaRouteTable::new(),
            raft_mgr,
            migrate_ctrl,
            node_state: Arc::new(Mutex::new(NodeState::default())),
        })
    }

    /// Bootstrap node and recover alive replicas.
    pub async fn bootstrap(&self, node_ident: &NodeIdent) -> Result<()> {
        use self::job::*;

        let mut node_state = self.node_state.lock().await;
        debug_assert!(
            node_state.replicas.is_empty(),
            "some replicas are serving before recovery?"
        );

        node_state.ident = Some(node_ident.to_owned());
        node_state.channel = Some(setup_report_state(self.provider.as_ref()));

        let node_id = node_ident.node_id;
        let it = self.provider.state_engine.iterate_replica_states().await;
        for (group_id, replica_id, state) in it {
            match state {
                ReplicaLocalState::Tombstone => continue,
                ReplicaLocalState::Terminated => {
                    setup_destory_replica(group_id, replica_id, self.provider.as_ref());
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
        info!(
            "create replica {replica_id} group {} with {} members",
            group.id,
            group.replicas.len()
        );

        let mut node_state = self.node_state.lock().await;
        if node_state.replicas.contains_key(&replica_id) {
            debug!(replica = replica_id, "replica already exists");
            return Ok(());
        }

        let group_id = group.id;
        if GroupEngine::open(group_id, self.provider.raw_db.clone())
            .await?
            .is_some()
        {
            warn!(
                new_replica = replica_id,
                "already exists a replica of the same group {group_id}"
            );
            return Err(Error::AlreadyExists(format!("group {group_id}")));
        }

        // To ensure crash-recovery consistency, first create raft metadata, and then create group
        // metadata. In this way, even if the group is restarted before the group is successfully
        // created, a replica can be recreated by retrying.
        Replica::create(replica_id, &group, &self.raft_mgr).await?;
        GroupEngine::create(self.provider.raw_db.clone(), &group).await?;
        let replica_state = if group.replicas.is_empty() {
            ReplicaLocalState::Pending
        } else {
            ReplicaLocalState::Initial
        };
        self.provider
            .state_engine
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
    pub async fn remove_replica(&self, replica_id: u64, actual_desc: &GroupDesc) -> Result<()> {
        let group_id = actual_desc.id;
        let replica = match self.replica_route_table.find(group_id) {
            Some(replica) => replica,
            None => {
                warn!(
                    group = group_id,
                    replica = replica_id,
                    "remove a not existed replica"
                );
                return Ok(());
            }
        };

        replica.shutdown(actual_desc).await?;
        if self.replica_route_table.remove(group_id).is_none() {
            return Ok(());
        }

        self.raft_route_table.delete(replica_id);

        let wait_group = {
            let mut node_state = self.node_state.lock().await;
            let ctx = node_state
                .replicas
                .remove(&replica_id)
                .expect("replica should exists");
            ctx.wait_group
        };

        wait_group.wait().await;

        // This replica is shutdowned, we need to update and persisted states.
        self.provider
            .state_engine
            .save_replica_state(group_id, replica_id, ReplicaLocalState::Terminated)
            .await?;

        // Clean group engine data in asynchronously.
        self::job::setup_destory_replica(group_id, replica_id, self.provider.as_ref());

        info!("remove replica {replica_id} of group {group_id} success");

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
        use self::replica::schedule;

        let group_engine = open_group_engine(self.provider.raw_db.clone(), group_id).await?;
        let wait_group = WaitGroup::new();
        let (sender, receiver) = mpsc::unbounded();

        let info = Arc::new(ReplicaInfo::new(&desc, group_id, local_state));
        let lease_state = Arc::new(std::sync::Mutex::new(LeaseState::new(
            group_engine.descriptor(),
            group_engine.migration_state(),
            sender,
        )));
        let raft_node = start_raft_group(
            &self.cfg,
            &self.raft_mgr,
            info.clone(),
            lease_state.clone(),
            channel,
            group_engine.clone(),
            wait_group.clone(),
        )
        .await?;

        let replica_id = info.replica_id;
        let replica = Replica::new(info, lease_state, raft_node.clone(), group_engine);
        let replica = Arc::new(replica);
        self.replica_route_table.update(replica.clone());
        self.raft_route_table.update(replica_id, raft_node);

        // Setup jobs
        self.migrate_ctrl
            .watch_state_changes(replica.clone(), receiver, wait_group.clone());
        schedule::setup(
            self.cfg.replica.clone(),
            self.provider.clone(),
            replica.clone(),
            wait_group.clone(),
        );

        Ok(ReplicaContext {
            info: replica.replica_info(),
            wait_group,
        })
    }

    /// Get root desc that known by node.
    pub async fn get_root(&self) -> RootDesc {
        // FIXME(walter) node_state might be locked by `create_replica`.
        self.node_state.lock().await.root.clone()
    }

    // Update recent known root nodes.
    pub async fn update_root(&self, root_desc: RootDesc) -> Result<()> {
        // TODO(walter) reject staled update root request.
        self.state_engine().save_root_desc(root_desc).await?;
        self.reload_root_from_engine().await
    }

    pub async fn reload_root_from_engine(&self) -> Result<()> {
        let root_desc = self
            .state_engine()
            .load_root_desc()
            .await?
            .ok_or_else(|| Error::InvalidData("root not found".into()))?;
        self.node_state.lock().await.root = root_desc;
        Ok(())
    }

    pub async fn execute_request(&self, request: GroupRequest) -> Result<GroupResponse> {
        use self::replica::retry::forwardable_execute;

        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };

        forwardable_execute(&self.migrate_ctrl, &replica, ExecCtx::default(), request).await
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
            self.cfg.shard_chunk_size,
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
        // replica.ingest(request.shard_id, ingest_chunk, true).await?;
        match replica.ingest(request.shard_id, ingest_chunk, true).await {
            Ok(_) | Err(Error::ShardNotFound(_)) => {
                // Ingest success or shard is migrated.
            }
            Err(e) => return Err(e),
        }

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

    // This request is issued by dest group.
    pub async fn migrate(&self, request: MigrateRequest) -> Result<MigrateResponse> {
        let desc = request
            .desc
            .ok_or_else(|| Error::InvalidArgument("MigrateRequest::desc".to_owned()))?;

        if desc.shard_desc.is_none() {
            return Err(Error::InvalidArgument(
                "MigrationDesc::shard_desc".to_owned(),
            ));
        }

        let group_id = desc.src_group_id;
        let replica = match self.replica_route_table.find(group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(group_id));
            }
        };

        loop {
            match MigrateAction::from_i32(request.action) {
                Some(MigrateAction::Setup) => {
                    match replica.setup_migration(&desc).await {
                        Ok(()) => {
                            return Ok(MigrateResponse {});
                        }
                        Err(Error::ServiceIsBusy(_)) => {
                            // already exists a migration task
                            crate::runtime::time::sleep(Duration::from_micros(200)).await;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };
                }
                Some(MigrateAction::Commit) => {
                    replica.commit_migration(&desc).await?;
                    return Ok(MigrateResponse {});
                }
                _ => return Err(Error::InvalidArgument("unknown action".to_owned())),
            }
        }
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
        &self.provider.state_engine
    }

    #[inline]
    pub fn executor(&self) -> &Executor {
        &self.provider.executor
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
                if info.group_id == ROOT_GROUP_ID {
                    continue;
                }
                let descriptor = replica.descriptor();
                if descriptor.replicas.is_empty() {
                    ns.orphan_replica_count += 1;
                }
                if descriptor.replicas.iter().any(|r| r.id == info.replica_id) {
                    // filter out the replica be removed by change_replica.
                    ns.group_count += 1;
                }
                let replica_state = replica.replica_state();
                if replica_state.role == RaftRole::Leader as i32 {
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
                if state.role == RaftRole::Leader as i32 {
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

    pub async fn collect_migration_state(
        &self,
        req: &CollectMigrationStateRequest,
    ) -> CollectMigrationStateResponse {
        use collect_migration_state_response::State;

        let mut resp = CollectMigrationStateResponse {
            state: State::None as i32,
            desc: None,
        };

        let group_id = req.group;
        if let Some(replica) = self.replica_route_table.find(group_id) {
            if !replica.replica_info().is_terminated() {
                if let Some(ms) = replica.migration_state() {
                    let mut state = match MigrationStep::from_i32(ms.step) {
                        Some(MigrationStep::Prepare) => State::Setup,
                        Some(MigrationStep::Migrated) => State::Migrated,
                        Some(MigrationStep::Migrating) => State::Migrating,
                        _ => State::None,
                    };
                    if ms.migration_desc.is_none() {
                        state = State::None;
                    }
                    resp.state = state as i32;
                    resp.desc = ms.migration_desc;
                }
            }
        }

        resp
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

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            shard_chunk_size: 64 * 1024 * 1024,
            shard_gc_keys: 256,
            replica: ReplicaConfig::default(),
        }
    }
}

async fn open_group_engine(raw_db: Arc<rocksdb::DB>, group_id: u64) -> Result<GroupEngine> {
    match GroupEngine::open(group_id, raw_db).await? {
        Some(group_engine) => Ok(group_engine),
        None => {
            panic!("no such group engine exists, group {group_id}");
        }
    }
}

async fn start_raft_group(
    cfg: &NodeConfig,
    raft_mgr: &RaftManager,
    info: Arc<ReplicaInfo>,
    lease_state: Arc<std::sync::Mutex<LeaseState>>,
    channel: StateChannel,
    group_engine: GroupEngine,
    wait_group: WaitGroup,
) -> Result<RaftNodeFacade> {
    let group_id = info.group_id;
    let state_observer = Box::new(LeaseStateObserver::new(
        info.clone(),
        lease_state.clone(),
        channel,
    ));
    let fsm = GroupStateMachine::new(
        cfg.replica.clone(),
        info.clone(),
        group_engine.clone(),
        state_observer.clone(),
    );
    raft_mgr
        .start_raft_group(
            group_id,
            info.replica_id,
            info.node_id,
            fsm,
            state_observer,
            wait_group.clone(),
        )
        .await
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use engula_api::server::v1::{report_request::GroupUpdates, ReplicaDesc, ReplicaRole};
    use tempdir::TempDir;

    use super::*;
    use crate::{bootstrap::INITIAL_EPOCH, runtime::ExecutorOwner};

    async fn create_node(root_dir: PathBuf, executor: Executor) -> Node {
        use crate::bootstrap::build_provider;

        let config = Config {
            root_dir,
            ..Default::default()
        };

        let provider = build_provider(&config, executor.clone()).await.unwrap();

        Node::new(config, provider).unwrap()
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
        let tmp_dir = TempDir::new("create_pending_replica").unwrap();

        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor).await;

            let group_id = 2;
            let replica_id = 2;
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![],
            };
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
        let tmp_dir = TempDir::new("create_replica").unwrap();

        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor).await;

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
            node.create_replica(replica_id, group).await.unwrap();

            assert!(matches!(
                replica_state(node, replica_id).await,
                Some(ReplicaLocalState::Initial),
            ));
        });
    }

    #[test]
    fn recover() {
        let tmp_dir = TempDir::new("recover-replica").unwrap();
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor.clone()).await;

            let group_id = 2;
            let replica_id = 2;
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![],
            };
            node.create_replica(replica_id, group).await.unwrap();
        });
        drop(executor);
        drop(executor_owner);

        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor.clone()).await;
            let ident = NodeIdent {
                cluster_id: vec![],
                node_id: 1,
            };
            node.bootstrap(&ident).await.unwrap();
        });
    }

    #[test]
    fn remove_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();

        let tmp_dir = TempDir::new("remove_replica").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor.clone()).await;

            let group_id = 2;
            let replica_id = 2;
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![],
            };
            node.create_replica(replica_id, group.clone())
                .await
                .unwrap();
            let ident = NodeIdent {
                cluster_id: vec![],
                node_id: 1,
            };
            node.bootstrap(&ident).await.unwrap();

            crate::runtime::time::sleep(Duration::from_millis(10)).await;

            node.remove_replica(replica_id, &group).await.unwrap();
        });
    }

    /// After removing a replica, rejoin a new replica of the same group.
    #[test]
    fn remove_and_add_replicas_in_the_same_group() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();

        let tmp_dir = TempDir::new("remove_replica_of_same_group").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor.clone()).await;

            let group_id = 2;
            let replica_id = 2;
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![],
            };
            node.create_replica(replica_id, group.clone())
                .await
                .unwrap();
            let ident = NodeIdent {
                cluster_id: vec![],
                node_id: 1,
            };
            node.bootstrap(&ident).await.unwrap();

            crate::runtime::time::sleep(Duration::from_millis(10)).await;

            node.remove_replica(replica_id, &group).await.unwrap();

            crate::runtime::time::sleep(Duration::from_millis(10)).await;

            let new_replica_id = 3;
            node.create_replica(new_replica_id, group.clone())
                .await
                .unwrap();
        });
    }

    #[test]
    fn try_add_replicas_in_the_same_group() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();

        let tmp_dir = TempDir::new("try_add_replicas_in_the_same_group").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor.clone()).await;

            let group_id = 2;
            let replica_id = 2;
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![],
            };
            node.create_replica(replica_id, group.clone())
                .await
                .unwrap();
            let ident = NodeIdent {
                cluster_id: vec![],
                node_id: 1,
            };
            node.bootstrap(&ident).await.unwrap();

            let new_replica_id = 3;
            let result = node.create_replica(new_replica_id, group.clone()).await;
            assert!(matches!(result, Err(Error::AlreadyExists(msg)) if msg.contains("group")));
        });
    }

    #[test]
    fn report_replica_state_after_creating_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();

        let tmp_dir = TempDir::new("report_replica_state_after_creating_replica").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned(), executor.clone()).await;

            let group_id = 2;
            let replica_id = 2;
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![],
            };
            node.create_replica(replica_id, group.clone())
                .await
                .unwrap();

            let (sender, mut receiver) = mpsc::unbounded();
            node.serve_replica(
                group_id,
                ReplicaDesc {
                    id: replica_id,
                    ..Default::default()
                },
                ReplicaLocalState::Normal,
                StateChannel::new(sender),
            )
            .await
            .unwrap();

            use futures::stream::StreamExt;

            let result = receiver.next().await;
            assert!(
                matches!(result, Some(GroupUpdates{ replica_state: Some(v), .. }) if v.replica_id == replica_id)
            );
        });
    }
}
