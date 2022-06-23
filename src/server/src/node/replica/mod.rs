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

mod acl;
mod eval;
pub mod fsm;
pub mod job;
pub mod retry;

use std::{
    sync::{atomic::AtomicI32, Arc, Mutex},
    task::{Poll, Waker},
};

use engula_api::{
    server::v1::{group_request_union::Request, group_response_union::Response, *},
    v1::{DeleteResponse, GetResponse, PutResponse},
};
use tracing::info;

use self::fsm::{DescObserver, GroupStateMachine};
use super::{engine::GroupEngine, job::StateChannel};
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

#[derive(Default)]
struct LeaseState {
    leader_id: u64,
    /// the largest term which state machine already known.
    applied_term: u64,
    replica_state: ReplicaState,
    descriptor: GroupDesc,
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
    ) -> Result<Self> {
        let info = Arc::new(ReplicaInfo::new(desc.id, group_id, local_state));
        let lease_state = Arc::new(Mutex::new(LeaseState {
            descriptor: group_engine.descriptor().unwrap(),
            ..Default::default()
        }));
        let state_observer = Box::new(LeaseStateObserver::new(
            info.clone(),
            lease_state.clone(),
            state_channel,
        ));
        let fsm = GroupStateMachine::new(group_engine.clone(), state_observer.clone());
        let raft_node = raft_mgr
            .start_raft_group(group_id, desc, fsm, state_observer)
            .await?;
        Ok(Replica {
            info,
            group_engine,
            raft_node,
            lease_state,
            meta_acl: Arc::default(),
        })
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
    pub(self) async fn execute(&self, group_request: &GroupRequest) -> Result<GroupResponse> {
        let request = group_request
            .request
            .as_ref()
            .and_then(|request| request.request.as_ref())
            .ok_or_else(|| Error::InvalidArgument("GroupRequest::request is None".into()))?;

        debug_assert_eq!(group_request.group_id, self.info.group_id);

        if self.info.is_terminated() {
            return Err(Error::GroupNotFound(self.info.group_id));
        }

        let _acl_guard = self.take_acl_guard(request).await;
        self.check_request_early(group_request.epoch, request)?;
        let resp = self.evaluate_command(request).await?;
        Ok(GroupResponse::new(resp))
    }

    pub async fn on_leader(&self, immediate: bool) -> Result<()> {
        if self.info.is_terminated() {
            return Err(Error::NotLeader(self.info.group_id, None));
        }

        use futures::future::poll_fn;

        poll_fn(|ctx| {
            let mut lease_state = self.lease_state.lock().unwrap();
            if lease_state.is_ready_for_serving() {
                Poll::Ready(Ok(()))
            } else if immediate || self.info.is_terminated() {
                Poll::Ready(Err(Error::NotLeader(self.info.group_id, None)))
            } else {
                lease_state.leader_subscribers.push(ctx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    /// Propose `SyncOp` to raft log.
    pub(super) async fn propose_sync_op(&self, op: SyncOp) -> Result<()> {
        self.check_leader_early()?;

        let eval_result = EvalResult {
            op: Some(op),
            ..Default::default()
        };
        self.raft_node.clone().propose(eval_result).await??;

        Ok(())
    }

    pub async fn fetch_shard_chunk(&self, shard_id: u64, last_key: &[u8]) -> Result<ShardChunk> {
        self.check_leader_early()?;

        let mut kvs = vec![];
        let mut size = 0;
        for (key, value) in self.group_engine.iter_from(shard_id, last_key)? {
            let key: Vec<_> = key.into();
            let value: Vec<_> = value.into();
            if key == last_key {
                continue;
            }
            size += key.len() + value.len();
            kvs.push(ShardData { key, value });
            if size > 64 * 1024 * 1024 {
                break;
            }
        }

        Ok(ShardChunk { data: kvs })
    }

    pub async fn ingest(&self, shard_id: u64, chunk: ShardChunk) -> Result<()> {
        use crate::node::engine::WriteBatch;

        if chunk.data.is_empty() {
            return Ok(());
        }

        self.check_leader_early()?;

        let mut wb = WriteBatch::default();
        for data in &chunk.data {
            self.group_engine
                .put(&mut wb, shard_id, &data.key, &data.value)?;
        }

        let ingest_event = migrate_event::Ingest {
            last_key: chunk.data.last().as_ref().unwrap().key.clone(),
        };
        let sync_op = SyncOp::migrate_event(migrate_event::Value::Ingest(ingest_event));
        let eval_result = EvalResult {
            batch: Some(WriteBatchRep {
                data: wb.data().to_owned(),
            }),
            op: Some(sync_op),
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
}

impl Replica {
    async fn take_acl_guard<'a>(&'a self, request: &'a Request) -> MetaAclGuard<'a> {
        if is_change_meta_request(request) {
            MetaAclGuard::Write(self.meta_acl.write().await)
        } else {
            MetaAclGuard::Read(self.meta_acl.read().await)
        }
    }

    /// Delegates the eval method for the given `Request`.
    async fn evaluate_command(&self, request: &Request) -> Result<Response> {
        let (eval_result_opt, resp) = match &request {
            Request::Get(req) => {
                let value = eval::get(&self.group_engine, req).await?;
                let resp = GetResponse { value };
                (None, Response::Get(resp))
            }
            Request::Put(req) => {
                let eval_result = eval::put(&self.group_engine, req).await?;
                (Some(eval_result), Response::Put(PutResponse {}))
            }
            Request::Delete(req) => {
                let eval_result = eval::delete(&self.group_engine, req).await?;
                (Some(eval_result), Response::Delete(DeleteResponse {}))
            }
            Request::PrefixList(req) => {
                let eval_result = eval::prefix_list(&self.group_engine, req).await?;
                (None, Response::PreifxList(eval_result))
            }
            Request::BatchWrite(req) => {
                let eval_result = eval::batch_write(&self.group_engine, req).await?;
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
            Request::MigrateShard(req) => {
                // TODO(walter) check migrate shard state
                let eval_result = eval::migrate(self.info.group_id, req).await?;
                let resp = MigrateShardResponse {};
                (Some(eval_result), Response::MigrateShard(resp))
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

    fn check_request_early(&self, epoch: u64, _request: &Request) -> Result<()> {
        let group_id = self.info.group_id;
        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.is_raft_leader() {
            Err(Error::NotLeader(group_id, lease_state.leader_descriptor()))
        } else if !lease_state.is_log_term_matched() {
            // Replica has just been elected as the leader, and there are still exists unapplied
            // WALs, so the freshness of metadata cannot be guaranteed.
            Err(Error::GroupNotReady(group_id))
        } else if epoch < lease_state.descriptor.epoch {
            Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
        } else {
            // If the current replica is the leader and has applied data in the current term,
            // it is expected that the input epoch should not be larger than the leaders.
            debug_assert_eq!(epoch, lease_state.descriptor.epoch);
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

impl LeaseState {
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

impl DescObserver for LeaseStateObserver {
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
        }
    }
}

pub(self) fn is_change_meta_request(request: &Request) -> bool {
    match request {
        Request::ChangeReplicas(_) | Request::CreateShard(_) | Request::MigrateShard(_) => true,
        Request::Get(_)
        | Request::Put(_)
        | Request::Delete(_)
        | Request::BatchWrite(_)
        | Request::PrefixList(_) => false,
    }
}
