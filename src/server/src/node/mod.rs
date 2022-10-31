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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use engula_api::server::v1::*;
use futures::{channel::mpsc, lock::Mutex};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use self::{
    engine::{EngineConfig, RawDb},
    job::StateChannel,
    migrate::{MigrateController, ShardChunkStream},
    replica::ReplicaConfig,
};
pub use self::{
    engine::{GroupEngine, StateEngine},
    replica::Replica,
    route_table::{RaftRouteTable, ReplicaRouteTable},
};
use crate::{
    constants::ROOT_GROUP_ID,
    node::replica::{fsm::GroupStateMachine, ExecCtx, LeaseState, LeaseStateObserver, ReplicaInfo},
    raftgroup::{snap::RecycleSnapMode, RaftManager, RaftNodeFacade, TransportManager},
    runtime::sync::WaitGroup,
    schedule::MoveReplicasProvider,
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

    #[serde(default)]
    pub replica: ReplicaConfig,

    #[serde(default)]
    pub engine: EngineConfig,
}

struct ReplicaContext {
    #[allow(dead_code)]
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

    serving_replicas: HashMap<u64, ReplicaContext>,

    /// `serving_groups` used to record the groups that has replicas being served on this node.
    /// Only one replica of a group is allowed on a node.
    serving_groups: HashSet<u64>,

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
    state_engine: StateEngine,

    /// Node related metadata, including serving replicas, root desc.
    node_state: Arc<Mutex<NodeState>>,

    /// A lock is used to ensure serialization of create/terminate replica operations.
    replica_mutation: Arc<Mutex<()>>,
}

impl Node {
    pub(crate) async fn new(
        cfg: Config,
        provider: Arc<Provider>,
        state_engine: StateEngine,
    ) -> Result<Self> {
        let raft_route_table = RaftRouteTable::new();
        let trans_mgr =
            TransportManager::build(provider.address_resolver.clone(), raft_route_table.clone())
                .await;
        let raft_mgr = RaftManager::open(
            cfg.raft.clone(),
            &provider.log_path,
            provider.raft_engine.clone(),
            trans_mgr,
        )
        .await?;
        let migrate_ctrl = MigrateController::new(cfg.node.clone(), provider.clone());
        Ok(Node {
            cfg: cfg.node,
            provider,
            raft_route_table,
            replica_route_table: ReplicaRouteTable::new(),
            raft_mgr,
            migrate_ctrl,
            state_engine,
            node_state: Arc::new(Mutex::new(NodeState::default())),
            replica_mutation: Arc::default(),
        })
    }

    /// Bootstrap node and recover alive replicas.
    pub async fn bootstrap(&self, node_ident: &NodeIdent) -> Result<()> {
        use self::job::*;

        let mut node_state = self.node_state.lock().await;
        debug_assert!(
            node_state.serving_replicas.is_empty(),
            "some replicas are serving before recovery?"
        );

        node_state.ident = Some(node_ident.to_owned());
        node_state.channel = Some(setup_report_state(self.provider.as_ref()));

        let node_id = node_ident.node_id;
        for (group_id, replica_id, state) in self.state_engine.replica_states().await? {
            if state == ReplicaLocalState::Terminated {
                setup_destory_replica(
                    group_id,
                    replica_id,
                    self.state_engine.clone(),
                    self.provider.raw_db.clone(),
                    self.raft_mgr.engine(),
                );
            }
            if matches!(
                state,
                ReplicaLocalState::Tombstone | ReplicaLocalState::Terminated
            ) {
                self.raft_mgr
                    .snapshot_manager()
                    .recycle_snapshots(replica_id, RecycleSnapMode::All);
                continue;
            }

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
            node_state.serving_replicas.insert(replica_id, context);
            node_state.serving_groups.insert(group_id);
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

        let group_id = group.id;
        let _mut_guard = self.replica_mutation.lock().await;
        if self.check_replica_existence(group_id, replica_id).await? {
            return Ok(());
        }

        // To ensure crash-recovery consistency, first create raft metadata, and then save replica
        // state. In this way, even if the node is restarted before the group is
        // successfully created, a replica can be recreated by retrying.
        Replica::create(replica_id, &group, &self.raft_mgr).await?;
        self.state_engine
            .save_replica_state(group_id, replica_id, ReplicaLocalState::Initial)
            .await?;

        info!("group {group_id} create replica {replica_id} and write initial state success");

        // If this node has not completed initialization, then there is no need to record
        // `ReplicaInfo`. Because the recovery operation will be performed later, `ReplicaMeta` will
        // be read again and the corresponding `ReplicaInfo` will be created.
        //
        // See `Node::bootstrap` for details.
        let mut node_state = self.node_state.lock().await;
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
                    ReplicaLocalState::Initial,
                    node_state.channel.as_ref().unwrap().clone(),
                )
                .await?;
            node_state.serving_replicas.insert(replica_id, context);
            node_state.serving_groups.insert(group_id);
        }

        Ok(())
    }

    async fn check_replica_existence(&self, group_id: u64, replica_id: u64) -> Result<bool> {
        let node_state = self.node_state.lock().await;
        if node_state.serving_replicas.contains_key(&replica_id) {
            debug!("group {group_id} create replica {replica_id}: already exists");
            return Ok(true);
        }

        if node_state.serving_groups.contains(&group_id) {
            warn!("group {group_id} create replica {replica_id}: already exists another replica");
            return Err(Error::AlreadyExists(format!("group {group_id}")));
        }

        Ok(false)
    }

    /// Remove the specified replica.
    pub async fn remove_replica(&self, replica_id: u64, actual_desc: &GroupDesc) -> Result<()> {
        let _mut_guard = self.replica_mutation.lock().await;

        let group_id = actual_desc.id;
        debug!("group {group_id} remove replica {replica_id}");
        let replica = match self.replica_route_table.find(group_id) {
            Some(replica) => replica,
            None => {
                warn!("group {group_id} remove replica {replica_id}: replica not existed");
                return Ok(());
            }
        };

        replica.shutdown(actual_desc).await?;
        self.replica_route_table.remove(group_id);
        self.raft_route_table.delete(replica_id);

        let wait_group = {
            let mut node_state = self.node_state.lock().await;
            node_state.serving_groups.remove(&group_id);
            let ctx = node_state
                .serving_replicas
                .remove(&replica_id)
                .expect("replica should exists before removing");
            ctx.wait_group
        };

        wait_group.wait().await;

        // This replica is shutdowned, we need to update and persisted states.
        self.state_engine
            .save_replica_state(group_id, replica_id, ReplicaLocalState::Terminated)
            .await?;

        self.raft_mgr
            .snapshot_manager()
            .recycle_snapshots(replica_id, RecycleSnapMode::All);

        // Clean group engine data in asynchronously.
        self::job::setup_destory_replica(
            group_id,
            replica_id,
            self.state_engine.clone(),
            self.provider.raw_db.clone(),
            self.raft_mgr.engine(),
        );

        info!("group {group_id} remove replica {replica_id} success");

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
        use crate::schedule::setup_scheduler;

        let group_engine = open_group_engine(
            &self.cfg.engine,
            self.provider.raw_db.clone(),
            group_id,
            desc.id,
            local_state,
        )
        .await?;
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
            channel.clone(),
            group_engine.clone(),
            wait_group.clone(),
        )
        .await?;

        let replica_id = info.replica_id;
        let move_replicas_provider = Arc::new(MoveReplicasProvider::new());
        let schedule_state_observer = Arc::new(LeaseStateObserver::new(
            info.clone(),
            lease_state.clone(),
            channel,
        ));
        let replica = Replica::new(
            info.clone(),
            lease_state,
            raft_node.clone(),
            group_engine,
            move_replicas_provider.clone(),
        );
        let replica = Arc::new(replica);
        self.replica_route_table.update(replica.clone());
        self.raft_route_table.update(replica_id, raft_node);

        // Setup jobs
        self.migrate_ctrl
            .watch_state_changes(replica.clone(), receiver, wait_group.clone())
            .await;

        setup_scheduler(
            self.cfg.replica.clone(),
            self.provider.clone(),
            replica.clone(),
            move_replicas_provider,
            schedule_state_observer,
            wait_group.clone(),
        );

        // Now that all initialization work is done, the replica is ready to serve, mark it as
        // normal state.
        if matches!(local_state, ReplicaLocalState::Initial) {
            info.as_normal_state();
            self.state_engine
                .save_replica_state(group_id, replica_id, ReplicaLocalState::Normal)
                .await?;
        }

        info!("group {group_id} replica {replica_id} is ready for serving");

        Ok(ReplicaContext { info, wait_group })
    }

    /// Get root desc that known by node.
    pub async fn get_root(&self) -> RootDesc {
        self.node_state.lock().await.root.clone()
    }

    // Update recent known root nodes.
    pub async fn update_root(&self, root_desc: RootDesc) -> Result<()> {
        let local_root_desc = self.get_root().await;
        if local_root_desc == root_desc {
            Ok(())
        } else {
            // TODO(walter) reject staled update root request.
            self.state_engine().save_root_desc(root_desc).await?;
            self.reload_root_from_engine().await
        }
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

    pub async fn execute_request(&self, request: &GroupRequest) -> Result<GroupResponse> {
        use self::replica::retry::forwardable_execute;

        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };

        forwardable_execute(&self.migrate_ctrl, &replica, &ExecCtx::default(), request).await
    }

    pub async fn pull_shard_chunks(&self, request: PullRequest) -> Result<ShardChunkStream> {
        let replica = match self.replica_route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };
        replica.check_migrating_request_early(request.shard_id)?;
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
        let resp = execute(&replica, &exec_ctx, &group_request).await?;
        debug_assert!(resp.response.is_some());
        Ok(ForwardResponse {
            response: resp.response,
        })
    }

    // This request is issued by dest group.
    pub async fn migrate(&self, request: MigrateRequest) -> Result<MigrateResponse> {
        use self::replica::retry::do_migration;

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

        let Some(action) = MigrateAction::from_i32(request.action) else {
            return Err(Error::InvalidArgument("unknown action".to_owned()));
        };

        do_migration(&replica, action, &desc).await?;
        Ok(MigrateResponse {})
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

    pub async fn collect_schedule_state(
        &self,
        _req: &CollectScheduleStateRequest,
    ) -> CollectScheduleStateResponse {
        let mut resp = CollectScheduleStateResponse {
            schedule_states: vec![],
        };

        for group_id in self.serving_group_id_list().await {
            if let Some(replica) = self.replica_route_table.find(group_id) {
                if !replica.replica_info().is_terminated()
                    && replica.replica_state().role == RaftRole::Leader as i32
                {
                    resp.schedule_states.push(replica.schedule_state());
                }
            }
        }

        resp
    }

    #[inline]
    async fn serving_group_id_list(&self) -> Vec<u64> {
        let node_state = self.node_state.lock().await;
        node_state.serving_groups.iter().cloned().collect()
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
            engine: EngineConfig::default(),
        }
    }
}

async fn open_group_engine(
    cfg: &EngineConfig,
    raw_db: Arc<RawDb>,
    group_id: u64,
    replica_id: u64,
    replica_state: ReplicaLocalState,
) -> Result<GroupEngine> {
    match GroupEngine::open(cfg, raw_db.clone(), group_id, replica_id).await? {
        Some(group_engine) => Ok(group_engine),
        None if matches!(replica_state, ReplicaLocalState::Initial) => {
            GroupEngine::create(cfg, raw_db, group_id, replica_id).await
        }
        None => {
            panic!("group {group_id} replica {replica_id} open group engine: no such group engine exists");
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

    use engula_api::{
        server::v1::{
            group_request_union::Request,
            report_request::GroupUpdates,
            shard_desc::{Partition, RangePartition},
            ReplicaDesc, ReplicaRole,
        },
        v1::PutRequest,
    };
    use tempdir::TempDir;

    use super::*;
    use crate::{constants::INITIAL_EPOCH, runtime::ExecutorOwner};

    async fn create_node(root_dir: PathBuf) -> Node {
        use crate::bootstrap::build_provider;

        let config = Config {
            root_dir,
            ..Default::default()
        };

        let (provider, state_engine) = build_provider(&config).await.unwrap();

        Node::new(config, provider, state_engine).await.unwrap()
    }

    async fn replica_state(node: Node, replica_id: u64) -> Option<ReplicaLocalState> {
        node.state_engine()
            .replica_states()
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, id, _)| *id == replica_id)
            .map(|(_, _, state)| state)
            .next()
    }

    #[test]
    fn create_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let tmp_dir = TempDir::new("create_replica").unwrap();

        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;

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
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;

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
        drop(executor_owner);

        let executor_owner = ExecutorOwner::new(1);
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;
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

        let tmp_dir = TempDir::new("remove_replica").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;

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

        let tmp_dir = TempDir::new("remove_replica_of_same_group").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;

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

        let tmp_dir = TempDir::new("try_add_replicas_in_the_same_group").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;

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

        let tmp_dir = TempDir::new("report_replica_state_after_creating_replica").unwrap();
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;

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
                ReplicaLocalState::Initial,
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

    #[test]
    fn create_after_removing_replica_with_same_group() {
        let tmp_dir = TempDir::new("create_after_removing_replica_with_same_group").unwrap();
        let executor_owner = ExecutorOwner::new(1);
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;
            node.bootstrap(&NodeIdent::default()).await.unwrap();

            let group_id = 2;
            let replica_id = 2;
            info!("create replica {replica_id} and remove it immediately");
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![ReplicaDesc {
                    id: replica_id,
                    node_id: 1,
                    role: ReplicaRole::Voter as i32,
                }],
            };
            node.create_replica(replica_id, group.clone())
                .await
                .unwrap();

            node.remove_replica(replica_id, &group).await.unwrap();

            let shard_id = 1;
            let new_replica_id = 3;
            info!("create new replica {new_replica_id}");
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![ShardDesc {
                    id: shard_id,
                    collection_id: 123,
                    partition: Some(Partition::Range(RangePartition::default())),
                }],
                replicas: vec![ReplicaDesc {
                    id: new_replica_id,
                    node_id: 1,
                    role: ReplicaRole::Voter as i32,
                }],
            };
            node.create_replica(new_replica_id, group.clone())
                .await
                .unwrap();

            let replica = node.replica_table().find(group_id).unwrap();
            replica.on_leader("test", false).await.unwrap();
            for _ in 0..100 {
                let mut ctx = ExecCtx::with_epoch(replica.epoch());
                let request = Request::Put(ShardPutRequest {
                    shard_id,
                    put: Some(PutRequest {
                        key: vec![0u8; 10],
                        value: vec![0u8; 10],
                    }),
                });
                replica.execute(&mut ctx, &request).await.unwrap();
            }
        });
    }

    #[test]
    fn bootstrap_and_create_after_removing_replica_with_same_group() {
        let tmp_dir =
            TempDir::new("bootstrap_and_create_after_removing_replica_with_same_group").unwrap();

        let group_id = 2;
        let executor_owner = ExecutorOwner::new(1);
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;
            node.bootstrap(&NodeIdent::default()).await.unwrap();

            let replica_id = 2;
            info!("create replica {replica_id} and remove it immediately");
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![],
                replicas: vec![ReplicaDesc {
                    id: replica_id,
                    node_id: 1,
                    role: ReplicaRole::Voter as i32,
                }],
            };
            node.create_replica(replica_id, group.clone())
                .await
                .unwrap();

            node.state_engine
                .save_replica_state(group_id, replica_id, ReplicaLocalState::Terminated)
                .await
                .unwrap();
        });

        drop(executor_owner);

        // Mock reboot.
        let executor_owner = ExecutorOwner::new(1);
        executor_owner.executor().block_on(async {
            let node = create_node(tmp_dir.path().to_owned()).await;
            node.bootstrap(&NodeIdent::default()).await.unwrap();

            let shard_id = 1;
            let new_replica_id = 3;
            info!("create new replica {new_replica_id}");
            let group = GroupDesc {
                id: group_id,
                epoch: INITIAL_EPOCH,
                shards: vec![ShardDesc {
                    id: shard_id,
                    collection_id: 123,
                    partition: Some(Partition::Range(RangePartition::default())),
                }],
                replicas: vec![ReplicaDesc {
                    id: new_replica_id,
                    node_id: 1,
                    role: ReplicaRole::Voter as i32,
                }],
            };
            node.create_replica(new_replica_id, group.clone())
                .await
                .unwrap();

            let replica = node.replica_table().find(group_id).unwrap();
            replica.on_leader("test", false).await.unwrap();
            for _ in 0..100 {
                let mut ctx = ExecCtx::with_epoch(replica.epoch());
                let request = Request::Put(ShardPutRequest {
                    shard_id,
                    put: Some(PutRequest {
                        key: vec![0u8; 10],
                        value: vec![0u8; 10],
                    }),
                });
                replica.execute(&mut ctx, &request).await.unwrap();
            }
        });
    }
}
