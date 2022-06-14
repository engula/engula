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
pub mod raft;

use std::{
    sync::{atomic::AtomicI32, Arc, Mutex},
    task::{Poll, Waker},
};

use engula_api::{
    server::v1::{
        group_request_union::Request, group_response_union::Response, BatchWriteResponse,
        ChangeReplicasResponse, CreateShardResponse, GroupDesc, GroupRequest, GroupResponse,
        ReplicaDesc,
    },
    v1::{DeleteResponse, GetResponse, PutResponse},
};

pub use self::raft::RaftNodeFacade as RaftSender;
use self::{
    fsm::GroupStateMachine,
    raft::{RaftManager, RaftNodeFacade, StateObserver},
};
use super::group_engine::GroupEngine;
use crate::{
    serverpb::v1::{EvalResult, ReplicaLocalState, SyncOp},
    Error, Result,
};

pub struct ReplicaInfo {
    pub replica_id: u64,
    pub group_id: u64,
    local_state: AtomicI32,
}

#[derive(Default)]
struct LeaseState {
    role: ::raft::StateRole,
    term: u64,
    leader_subscribers: Vec<Waker>,
}

struct RoleObserver {
    lease_state: Arc<Mutex<LeaseState>>,
}

pub struct Replica
where
    Self: Send,
{
    info: Arc<ReplicaInfo>,
    group_engine: GroupEngine,
    raft_node: RaftNodeFacade,
    lease_state: Arc<Mutex<LeaseState>>,
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
        raft::write_initial_state(raft_mgr.engine(), replica_id, voters, eval_results).await?;
        Ok(())
    }

    /// Open the existed replica of raft group.
    pub async fn recover(
        group_id: u64,
        desc: ReplicaDesc,
        local_state: ReplicaLocalState,
        group_engine: GroupEngine,
        raft_mgr: &RaftManager,
    ) -> Result<Self> {
        let info = Arc::new(ReplicaInfo::new(desc.id, group_id, local_state));
        let fsm = GroupStateMachine::new(group_engine.clone());
        let lease_state: Arc<Mutex<LeaseState>> = Arc::default();
        let observer = Box::new(RoleObserver::new(lease_state.clone()));
        let raft_node = raft_mgr
            .start_raft_group(group_id, desc, fsm, observer)
            .await?;
        Ok(Replica {
            info,
            group_engine,
            raft_node,
            lease_state,
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
    pub async fn execute(&self, group_request: &GroupRequest) -> Result<GroupResponse> {
        if self.info.is_terminated() {
            return Err(Error::GroupNotFound(self.info.group_id));
        }

        // TODO(walter) check request epoch.

        let group_id = group_request.group_id;
        let shard_id = group_request.shard_id;
        debug_assert_eq!(group_id, self.info.group_id);

        let request = group_request
            .request
            .as_ref()
            .and_then(|request| request.request.as_ref())
            .ok_or_else(|| Error::InvalidArgument("GroupRequest::request".into()))?;

        self.check_request_early(group_id, request)?;
        let resp = self.evaluate_command(shard_id, request).await?;
        Ok(GroupResponse::new(resp))
    }

    pub async fn on_leader(&self) -> Result<()> {
        if self.info.is_terminated() {
            return Err(Error::NotLeader(self.info.group_id, None));
        }

        use futures::future::poll_fn;

        poll_fn(|ctx| {
            let mut lease_state = self.lease_state.lock().unwrap();
            if lease_state.still_valid() {
                Poll::Ready(())
            } else {
                lease_state.leader_subscribers.push(ctx.waker().clone());
                Poll::Pending
            }
        })
        .await;

        // FIXME(walter) support shutdown a replica.

        Ok(())
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

    #[inline]
    pub fn replica_info(&self) -> Arc<ReplicaInfo> {
        self.info.clone()
    }

    #[inline]
    pub fn raft_node(&self) -> RaftNodeFacade {
        self.raft_node.clone()
    }
}

impl Replica {
    /// Delegates the eval method for the given `Request`.
    async fn evaluate_command(&self, shard_id: u64, request: &Request) -> Result<Response> {
        let resp: Response;
        let eval_result_opt = match &request {
            Request::Get(get) => {
                let value = eval::get(&self.group_engine, shard_id, &get.key).await?;
                resp = Response::Get(GetResponse {
                    value: value.map(|v| v.to_vec()),
                });
                None
            }
            Request::Put(req) => {
                resp = Response::Put(PutResponse {});
                let eval_result =
                    eval::put(&self.group_engine, shard_id, &req.key, &req.value).await?;
                Some(eval_result)
            }
            Request::Delete(req) => {
                resp = Response::Delete(DeleteResponse {});
                let eval_result = eval::delete(&self.group_engine, shard_id, &req.key).await?;
                Some(eval_result)
            }
            Request::BatchWrite(req) => {
                resp = Response::BatchWrite(BatchWriteResponse {});
                eval::batch_write(&self.group_engine, shard_id, req).await?
            }
            Request::CreateShard(req) => {
                // TODO(walter) check the existing of shard.
                let shard = req
                    .shard
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| Error::InvalidArgument("CreateShard::shard".into()))?;
                resp = Response::CreateShard(CreateShardResponse {});

                Some(eval::add_shard(shard))
            }
            Request::ChangeReplicas(req) => {
                if let Some(change) = &req.change_replicas {
                    self.raft_node
                        .clone()
                        .change_config(change.clone())
                        .await??;
                }
                return Ok(Response::ChangeReplicas(ChangeReplicasResponse {}));
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

    fn check_request_early(&self, group_id: u64, _request: &Request) -> Result<()> {
        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.still_valid() {
            Err(Error::NotLeader(group_id, None))
        } else {
            Ok(())
        }
    }

    fn check_leader_early(&self) -> Result<()> {
        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.still_valid() {
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
                .compare_exchange(
                    local_state,
                    TERMINATED,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .into_ok_or_err();
        }
    }
}

impl LeaseState {
    fn still_valid(&self) -> bool {
        use ::raft::StateRole;
        self.role == StateRole::Leader
    }

    fn wake_all_waiters(&mut self) {
        for waker in std::mem::take(&mut self.leader_subscribers) {
            waker.wake();
        }
    }
}

impl RoleObserver {
    fn new(lease_state: Arc<Mutex<LeaseState>>) -> Self {
        RoleObserver { lease_state }
    }
}

impl StateObserver for RoleObserver {
    fn on_state_updated(&mut self, _leader_id: u64, term: u64, role: ::raft::StateRole) {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.role = role;
        lease_state.term = term;
        if role == ::raft::StateRole::Leader {
            lease_state.wake_all_waiters();
        }
    }
}
