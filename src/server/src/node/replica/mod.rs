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

mod eval;
pub mod fsm;
pub mod job;
mod migrate;
pub mod retry;

use std::{
    sync::{atomic::AtomicI32, Arc, Mutex},
    task::{Poll, Waker},
};

use engula_api::{
    server::v1::{group_request_union::Request, group_response_union::Response, *},
    v1::{DeleteResponse, GetResponse, PutResponse},
};
use futures::channel::mpsc;
use tracing::info;

use self::fsm::{GroupStateMachine, StateMachineObserver};
use super::{engine::GroupEngine, job::StateChannel, migrate::MigrateController};
pub use crate::raftgroup::RaftNodeFacade as RaftSender;
use crate::{
    raftgroup::{write_initial_state, RaftManager, RaftNodeFacade, StateObserver},
    serverpb::v1::*,
    Error, Result,
};

pub struct ReplicaInfo {
    pub replica_id: u64,
    pub group_id: u64,
    local_state: AtomicI32,
}

struct LeaseState {
    leader_id: u64,
    /// the largest term which state machine already known.
    applied_term: u64,
    replica_state: ReplicaState,
    descriptor: GroupDesc,
    migration_state: Option<MigrationState>,
    migration_state_subscriber: mpsc::UnboundedSender<MigrationState>,
    leader_subscribers: Vec<Waker>,
}

/// A struct that observes changes to `GroupDesc` and `ReplicaState` , and broadcasts those changes
/// while saving them to `LeaseState`.
#[derive(Clone)]
struct LeaseStateObserver {
    info: Arc<ReplicaInfo>,
    lease_state: Arc<Mutex<LeaseState>>,
    state_channel: StateChannel,
}

enum MetaAclGuard<'a> {
    Read(tokio::sync::RwLockReadGuard<'a, ()>),
    Write(tokio::sync::RwLockWriteGuard<'a, ()>),
}

/// ExecCtx contains the required infos during request execution.
#[derive(Default, Clone)]
pub struct ExecCtx {
    /// This is a forward request and here is the migrating shard.
    pub forward_shard_id: Option<u64>,
    /// The epoch of `GroupDesc` carried in this request.
    pub epoch: u64,

    /// The migration desc, filled by `check_request_early`.
    migration_desc: Option<MigrationDesc>,
}

pub struct Replica
where
    Self: Send,
{
    info: Arc<ReplicaInfo>,
    group_engine: GroupEngine,
    raft_node: RaftNodeFacade,
    lease_state: Arc<Mutex<LeaseState>>,
    meta_acl: Arc<tokio::sync::RwLock<()>>,
}

impl Replica {
    /// Create new instance of the specified raft node.
    pub async fn create(
        replica_id: u64,
        target_desc: &GroupDesc,
        raft_mgr: &RaftManager,
    ) -> Result<()> {
        let voters = target_desc
            .replicas
            .iter()
            .map(|r| (r.id, r.node_id))
            .collect::<Vec<_>>();
        let eval_results = target_desc
            .shards
            .iter()
            .cloned()
            .map(eval::add_shard)
            .collect::<Vec<_>>();
        write_initial_state(raft_mgr.engine(), replica_id, voters, eval_results).await?;
        Ok(())
    }

    /// Open the existed replica of raft group.
    pub async fn recover(
        group_id: u64,
        desc: ReplicaDesc,
        local_state: ReplicaLocalState,
        state_channel: StateChannel,
        group_engine: GroupEngine,
        raft_mgr: &RaftManager,
        migrate_ctrl: MigrateController,
    ) -> Result<Arc<Self>> {
        let (sender, receiver) = mpsc::unbounded();
        let info = Arc::new(ReplicaInfo::new(desc.id, group_id, local_state));
        let lease_state = Arc::new(Mutex::new(LeaseState::new(&group_engine, sender)));
        let state_observer = Box::new(LeaseStateObserver::new(
            info.clone(),
            lease_state.clone(),
            state_channel,
        ));
        let fsm =
            GroupStateMachine::new(info.clone(), group_engine.clone(), state_observer.clone());
        let raft_node = raft_mgr
            .start_raft_group(group_id, desc, fsm, state_observer)
            .await?;

        let replica = Arc::new(Replica {
            info,
            group_engine,
            raft_node,
            lease_state,
            meta_acl: Arc::default(),
        });
        migrate_ctrl.watch_state_changes(replica.clone(), receiver);

        use crate::runtime::TaskPriority;

        let tag_owner = group_id.to_le_bytes();
        let tag = Some(tag_owner.as_slice());
        let router = migrate_ctrl.router();
        let cloned_replica = replica.clone();
        raft_mgr
            .executor()
            .spawn(tag, TaskPriority::IoHigh, async move {
                job::scheduler_main(router, cloned_replica).await;
            });

        Ok(replica)
    }

    /// Shutdown this replicas with the newer `GroupDesc`.
    pub async fn shutdown(&self, _actual_desc: &GroupDesc) -> Result<()> {
        // TODO(walter) check actual desc.
        self.info.terminate();

        {
            let mut lease_state = self.lease_state.lock().unwrap();
            lease_state.wake_all_waiters();
        }

        // TODO(walter) blocks until all asynchronously task finished.

        Ok(())
    }
}

impl Replica {
    /// Execute group request and fill response.
    pub(self) async fn execute(
        &self,
        mut exec_ctx: ExecCtx,
        request: &Request,
    ) -> Result<Response> {
        if self.info.is_terminated() {
            return Err(Error::GroupNotFound(self.info.group_id));
        }

        let _acl_guard = self.take_acl_guard(request).await;
        self.check_request_early(&mut exec_ctx, request)?;
        self.evaluate_command(&exec_ctx, request).await
    }

    pub async fn on_leader(&self, immediate: bool) -> Result<Option<u64>> {
        if self.info.is_terminated() {
            return Err(Error::NotLeader(self.info.group_id, None));
        }

        use futures::future::poll_fn;

        poll_fn(|ctx| {
            let mut lease_state = self.lease_state.lock().unwrap();
            if lease_state.is_ready_for_serving() {
                Poll::Ready(Ok(Some(lease_state.replica_state.term)))
            } else if immediate {
                Poll::Ready(Ok(None))
            } else if self.info.is_terminated() {
                Poll::Ready(Err(Error::NotLeader(self.info.group_id, None)))
            } else {
                lease_state.leader_subscribers.push(ctx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    /// Propose `SyncOp` to raft log.
    pub(super) async fn propose_sync_op(&self, op: Box<SyncOp>) -> Result<()> {
        self.check_leader_early()?;

        let eval_result = EvalResult {
            op: Some(op),
            ..Default::default()
        };
        self.raft_node.clone().propose(eval_result).await??;

        Ok(())
    }

    #[inline]
    pub fn replica_info(&self) -> Arc<ReplicaInfo> {
        self.info.clone()
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.lease_state.lock().unwrap().descriptor.epoch
    }

    #[inline]
    pub fn raft_node(&self) -> RaftNodeFacade {
        self.raft_node.clone()
    }

    #[inline]
    pub fn descriptor(&self) -> GroupDesc {
        self.lease_state.lock().unwrap().descriptor.clone()
    }

    #[inline]
    pub fn replica_state(&self) -> ReplicaState {
        self.lease_state.lock().unwrap().replica_state.clone()
    }

    #[inline]
    pub fn group_engine(&self) -> GroupEngine {
        self.group_engine.clone()
    }
}

impl Replica {
    #[inline]
    async fn take_acl_guard<'a>(&'a self, request: &'a Request) -> MetaAclGuard<'a> {
        if is_change_meta_request(request) {
            self.take_write_acl_guard().await
        } else {
            self.take_read_acl_guard().await
        }
    }

    #[inline]
    async fn take_write_acl_guard(&self) -> MetaAclGuard {
        MetaAclGuard::Write(self.meta_acl.write().await)
    }

    #[inline]
    async fn take_read_acl_guard(&self) -> MetaAclGuard {
        MetaAclGuard::Read(self.meta_acl.read().await)
    }

    /// Delegates the eval method for the given `Request`.
    async fn evaluate_command(&self, exec_ctx: &ExecCtx, request: &Request) -> Result<Response> {
        let (eval_result_opt, resp) = match &request {
            Request::Get(req) => {
                let value = eval::get(exec_ctx, &self.group_engine, req).await?;
                let resp = GetResponse { value };
                (None, Response::Get(resp))
            }
            Request::Put(req) => {
                let eval_result = eval::put(exec_ctx, &self.group_engine, req).await?;
                (Some(eval_result), Response::Put(PutResponse {}))
            }
            Request::Delete(req) => {
                let eval_result = eval::delete(exec_ctx, &self.group_engine, req).await?;
                (Some(eval_result), Response::Delete(DeleteResponse {}))
            }
            Request::PrefixList(req) => {
                let eval_result = eval::prefix_list(&self.group_engine, req).await?;
                (None, Response::PrefixList(eval_result))
            }
            Request::BatchWrite(req) => {
                let eval_result = eval::batch_write(exec_ctx, &self.group_engine, req).await?;
                (eval_result, Response::BatchWrite(BatchWriteResponse {}))
            }
            Request::CreateShard(req) => {
                // TODO(walter) check the existing of shard.
                let shard = req
                    .shard
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| Error::InvalidArgument("CreateShard::shard".into()))?;
                let resp = CreateShardResponse {};
                (Some(eval::add_shard(shard)), Response::CreateShard(resp))
            }
            Request::ChangeReplicas(req) => {
                if let Some(change) = &req.change_replicas {
                    self.raft_node
                        .clone()
                        .change_config(change.clone())
                        .await??;
                }
                let resp = ChangeReplicasResponse {};
                (None, Response::ChangeReplicas(resp))
            }
            Request::AcceptShard(req) => {
                let eval_result = eval::accept_shard(self.info.group_id, exec_ctx.epoch, req).await;
                let resp = AcceptShardResponse {};
                (Some(eval_result), Response::AcceptShard(resp))
            }
        };

        if let Some(eval_result) = eval_result_opt {
            self.propose(eval_result).await?;
        }

        Ok(resp)
    }

    async fn propose(&self, eval_result: EvalResult) -> Result<()> {
        self.raft_node.clone().propose(eval_result).await??;
        Ok(())
    }

    fn check_request_early(&self, exec_ctx: &mut ExecCtx, req: &Request) -> Result<()> {
        let group_id = self.info.group_id;
        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.is_raft_leader() {
            Err(Error::NotLeader(group_id, lease_state.leader_descriptor()))
        } else if !lease_state.is_log_term_matched() {
            // Replica has just been elected as the leader, and there are still exists unapplied
            // WALs, so the freshness of metadata cannot be guaranteed.
            Err(Error::GroupNotReady(group_id))
        } else if exec_ctx.forward_shard_id.is_none()
            && exec_ctx.epoch < lease_state.descriptor.epoch
        {
            Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
        } else if let Some(shard_id) = exec_ctx.forward_shard_id {
            if lease_state.is_migrating_shard(shard_id) {
                Ok(())
            } else {
                // FIXME(walter) maybe migration is finished!!!!
                // Maybe this request has expired?
                Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
            }
        } else if lease_state.is_migrating() && matches!(req, Request::AcceptShard(_)) {
            // At the same time, there can only be one migration task.
            Err(Error::ServiceIsBusy("migration"))
        } else {
            // If the current replica is the leader and has applied data in the current term,
            // it is expected that the input epoch should not be larger than the leaders.
            debug_assert_eq!(exec_ctx.epoch, lease_state.descriptor.epoch);
            let migrating_digest = lease_state
                .migration_state
                .as_ref()
                .and_then(|m| m.migration_desc.clone());
            exec_ctx.migration_desc = migrating_digest;
            Ok(())
        }
    }

    fn check_leader_early(&self) -> Result<()> {
        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.is_ready_for_serving() {
            Err(Error::NotLeader(self.info.group_id, None))
        } else {
            Ok(())
        }
    }
}

impl ReplicaInfo {
    pub fn new(replica_id: u64, group_id: u64, local_state: ReplicaLocalState) -> Self {
        ReplicaInfo {
            replica_id,
            group_id,
            local_state: AtomicI32::new(local_state.into()),
        }
    }

    #[inline]
    pub fn local_state(&self) -> ReplicaLocalState {
        use std::sync::atomic::Ordering;
        ReplicaLocalState::from_i32(self.local_state.load(Ordering::Acquire)).unwrap()
    }

    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.local_state() == ReplicaLocalState::Terminated
    }

    #[inline]
    pub fn terminate(&self) {
        use std::sync::atomic::Ordering;

        const TERMINATED: i32 = ReplicaLocalState::Terminated as i32;
        let mut local_state: i32 = self.local_state().into();
        while local_state == TERMINATED {
            local_state = self
                .local_state
                .compare_exchange(local_state, TERMINATED, Ordering::AcqRel, Ordering::Acquire)
                .into_ok_or_err();
        }
    }
}

impl ExecCtx {
    pub fn forward(shard_id: u64) -> Self {
        ExecCtx {
            forward_shard_id: Some(shard_id),
            ..Default::default()
        }
    }

    #[inline]
    fn is_migrating_shard(&self, shard_id: u64) -> bool {
        self.migration_desc
            .as_ref()
            .and_then(|m| m.shard_desc.as_ref())
            .map(|d| d.id == shard_id)
            .unwrap_or_default()
    }
}

impl LeaseState {
    fn new(
        group_engine: &GroupEngine,
        migration_state_subscriber: mpsc::UnboundedSender<MigrationState>,
    ) -> Self {
        LeaseState {
            descriptor: group_engine.descriptor(),
            migration_state: group_engine.migration_state(),
            migration_state_subscriber,
            leader_id: 0,
            applied_term: 0,
            replica_state: ReplicaState::default(),
            leader_subscribers: vec![],
        }
    }

    #[inline]
    fn is_raft_leader(&self) -> bool {
        self.replica_state.role == RaftRole::Leader.into()
    }

    /// At least one log for the current term has been applied?
    #[inline]
    fn is_log_term_matched(&self) -> bool {
        self.applied_term == self.replica_state.term
    }

    #[inline]
    fn is_ready_for_serving(&self) -> bool {
        self.is_raft_leader() && self.is_log_term_matched()
    }

    #[inline]
    fn is_migrating(&self) -> bool {
        self.migration_state.is_some()
    }

    #[inline]
    fn is_migrating_shard(&self, shard_id: u64) -> bool {
        self.migration_state
            .as_ref()
            .map(|s| s.get_shard_id() == shard_id)
            .unwrap_or_default()
    }

    #[inline]
    fn is_same_migration(&self, desc: &MigrationDesc) -> bool {
        self.migration_state.as_ref().unwrap().get_migration_desc() == desc
    }

    #[inline]
    fn wake_all_waiters(&mut self) {
        for waker in std::mem::take(&mut self.leader_subscribers) {
            waker.wake();
        }
    }

    #[inline]
    fn leader_descriptor(&self) -> Option<ReplicaDesc> {
        self.descriptor
            .replicas
            .iter()
            .find(|r| r.id == self.leader_id)
            .cloned()
    }
}

impl LeaseStateObserver {
    fn new(
        info: Arc<ReplicaInfo>,
        lease_state: Arc<Mutex<LeaseState>>,
        state_channel: StateChannel,
    ) -> Self {
        LeaseStateObserver {
            info,
            lease_state,
            state_channel,
        }
    }

    fn update_replica_state(
        &self,
        leader_id: u64,
        voted_for: u64,
        term: u64,
        role: RaftRole,
    ) -> (ReplicaState, Option<GroupDesc>) {
        let replica_state = ReplicaState {
            replica_id: self.info.replica_id,
            group_id: self.info.group_id,
            term,
            voted_for,
            role: role.into(),
        };
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.leader_id = leader_id;
        lease_state.replica_state = replica_state.clone();
        let desc = if role == RaftRole::Leader {
            info!(
                "replica {} become leader of group {} at term {}",
                self.info.replica_id, self.info.group_id, term
            );
            Some(lease_state.descriptor.clone())
        } else {
            None
        };
        (replica_state, desc)
    }

    fn update_descriptor(&self, descriptor: GroupDesc) -> bool {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.descriptor = descriptor;
        lease_state.replica_state.role == RaftRole::Leader.into()
    }
}

impl StateObserver for LeaseStateObserver {
    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole) {
        let (state, desc) = self.update_replica_state(leader_id, voted_for, term, role);
        self.state_channel
            .broadcast_replica_state(self.info.group_id, state);
        if let Some(desc) = desc {
            self.state_channel
                .broadcast_group_descriptor(self.info.group_id, desc);
        }
    }
}

impl StateMachineObserver for LeaseStateObserver {
    fn on_descriptor_updated(&mut self, descriptor: GroupDesc) {
        if self.update_descriptor(descriptor.clone()) {
            self.state_channel
                .broadcast_group_descriptor(self.info.group_id, descriptor);
        }
    }

    fn on_term_updated(&mut self, term: u64) {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.applied_term = term;
        if lease_state.is_ready_for_serving() {
            info!(
                "replica {} is ready for serving requests of group {} at term {}",
                self.info.replica_id, self.info.group_id, term
            );
            lease_state.wake_all_waiters();
            if let Some(migration_state) = lease_state.migration_state.as_ref() {
                lease_state
                    .migration_state_subscriber
                    .unbounded_send(migration_state.to_owned())
                    .unwrap_or_default();
            }
        }
    }

    fn on_migrate_state_updated(&mut self, migration_state: Option<MigrationState>) {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.migration_state = migration_state;
        if let Some(migration_state) = lease_state.migration_state.as_ref() {
            if lease_state.is_ready_for_serving() {
                lease_state
                    .migration_state_subscriber
                    .unbounded_send(migration_state.to_owned())
                    .unwrap_or_default();
            }
        }
    }
}

pub(self) fn is_change_meta_request(request: &Request) -> bool {
    match request {
        Request::ChangeReplicas(_) | Request::CreateShard(_) | Request::AcceptShard(_) => true,
        Request::Get(_)
        | Request::Put(_)
        | Request::Delete(_)
        | Request::BatchWrite(_)
        | Request::PrefixList(_) => false,
    }
}
