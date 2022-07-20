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
mod migrate;
pub mod retry;
pub mod schedule;
mod state;

use std::{
    sync::{atomic::AtomicI32, Arc, Mutex},
    task::Poll,
};

use engula_api::{
    server::v1::{group_request_union::Request, group_response_union::Response, *},
    v1::{DeleteResponse, GetResponse, PutResponse},
};
use serde::{Deserialize, Serialize};

pub use self::state::{LeaseState, LeaseStateObserver};
use super::engine::GroupEngine;
pub use crate::raftgroup::RaftNodeFacade as RaftSender;
use crate::{
    raftgroup::{write_initial_state, RaftManager, RaftNodeFacade, ReadPolicy},
    serverpb::v1::*,
    Error, Result,
};

#[derive(Clone, Debug, Default)]
pub struct ReplicaTestingKnobs {
    pub disable_orphan_replica_detecting_intervals: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicaConfig {
    /// The limit size of each snapshot files.
    ///
    /// Default: 64MB.
    pub snap_file_size: u64,

    #[serde(skip)]
    pub testing_knobs: ReplicaTestingKnobs,
}

pub struct ReplicaInfo {
    pub replica_id: u64,
    pub group_id: u64,
    pub node_id: u64,
    local_state: AtomicI32,
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
    pub fn new(
        info: Arc<ReplicaInfo>,
        lease_state: Arc<Mutex<LeaseState>>,
        raft_node: RaftNodeFacade,
        group_engine: GroupEngine,
    ) -> Self {
        Replica {
            info,
            group_engine,
            raft_node,
            lease_state,
            meta_acl: Arc::default(),
        }
    }

    /// Shutdown this replicas with the newer `GroupDesc`.
    pub async fn shutdown(&self, _actual_desc: &GroupDesc) -> Result<()> {
        // TODO(walter) check actual desc.
        self.info.terminate();
        self.raft_node.clone().terminate();

        {
            let mut lease_state = self.lease_state.lock().unwrap();
            lease_state.terminate();
        }

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
        use futures::future::poll_fn;

        if self.info.is_terminated() {
            return Err(Error::NotLeader(self.info.group_id, None));
        }

        poll_fn(|ctx| {
            let mut lease_state = self.lease_state.lock().unwrap();
            if lease_state.is_ready_for_serving() {
                Poll::Ready(Ok(Some(lease_state.replica_state.term)))
            } else if immediate {
                Poll::Ready(Ok(None))
            } else if self.info.is_terminated() {
                Poll::Ready(Err(Error::NotLeader(self.info.group_id, None)))
            } else {
                // FIXME(walter) remove dup wakers.
                lease_state.leader_subscribers.push(ctx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    /// Check if the leader still hold the lease?
    pub async fn check_lease(&self) -> Result<()> {
        self.check_leader_early()?;
        self.raft_node.clone().read(ReadPolicy::ReadIndex).await?;
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
                    self.raft_node.clone().change_config(change.clone()).await?;
                }
                let resp = ChangeReplicasResponse {};
                (None, Response::ChangeReplicas(resp))
            }
            Request::AcceptShard(req) => {
                let eval_result = eval::accept_shard(self.info.group_id, exec_ctx.epoch, req).await;
                let resp = AcceptShardResponse {};
                (Some(eval_result), Response::AcceptShard(resp))
            }
            Request::Transfer(req) => {
                self.raft_node.clone().transfer_leader(req.transferee)?;
                return Ok(Response::Transfer(TransferResponse {}));
            }
        };

        if let Some(eval_result) = eval_result_opt {
            self.propose(eval_result).await?;
        }

        Ok(resp)
    }

    async fn propose(&self, eval_result: EvalResult) -> Result<()> {
        self.raft_node.clone().propose(eval_result).await?;
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
    pub fn new(replica_desc: &ReplicaDesc, group_id: u64, local_state: ReplicaLocalState) -> Self {
        let replica_id = replica_desc.id;
        let node_id = replica_desc.node_id;
        ReplicaInfo {
            replica_id,
            node_id,
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
        while local_state != TERMINATED {
            local_state = self
                .local_state
                .compare_exchange(local_state, TERMINATED, Ordering::AcqRel, Ordering::Acquire)
                .into_ok_or_err();
        }
    }
}

impl ExecCtx {
    pub fn with_epoch(epoch: u64) -> Self {
        ExecCtx {
            epoch,
            ..Default::default()
        }
    }

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

impl Default for ReplicaConfig {
    fn default() -> Self {
        ReplicaConfig {
            snap_file_size: 64 * 1024 * 1024 * 1024,
            testing_knobs: ReplicaTestingKnobs::default(),
        }
    }
}

pub(self) fn is_change_meta_request(request: &Request) -> bool {
    match request {
        Request::ChangeReplicas(_)
        | Request::CreateShard(_)
        | Request::AcceptShard(_)
        | Request::Transfer(_) => true,
        Request::Get(_)
        | Request::Put(_)
        | Request::Delete(_)
        | Request::BatchWrite(_)
        | Request::PrefixList(_) => false,
    }
}
