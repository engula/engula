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
pub mod raft;

use std::{
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use engula_api::{
    server::v1::{
        group_request_union::Request, group_response_union::Response, CreateShardResponse,
        GroupDesc, GroupRequest, GroupResponse,
    },
    v1::{DeleteResponse, GetResponse, PutResponse},
};

pub use self::raft::RaftNodeFacade as RaftSender;
use self::{
    fsm::GroupStateMachine,
    raft::{RaftManager, RaftNodeFacade, StateObserver},
};
use super::group_engine::GroupEngine;
use crate::{serverpb::v1::EvalResult, Error, Result};

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
    replica_id: u64,
    group_id: u64,
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
            .map(|r| r.id)
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
        replica_id: u64,
        group_engine: GroupEngine,
        raft_mgr: &RaftManager,
    ) -> Result<Self> {
        let fsm = GroupStateMachine::new(group_engine.clone());
        let lease_state: Arc<Mutex<LeaseState>> = Arc::default();
        let observer = Box::new(RoleObserver::new(lease_state.clone()));
        let raft_node = raft_mgr
            .start_raft_group(group_id, replica_id, fsm, observer)
            .await?;
        Ok(Replica {
            replica_id,
            group_id,
            group_engine,
            raft_node,
            lease_state,
        })
    }

    /// Execute group request and fill response.
    pub async fn execute(&self, group_request: &GroupRequest) -> Result<GroupResponse> {
        let group_id = group_request.group_id;
        let shard_id = group_request.shard_id;
        let request = group_request
            .request
            .as_ref()
            .and_then(|request| request.request.as_ref())
            .ok_or_else(|| Error::InvalidArgument("GroupRequest::request".into()))?;

        self.check_request_early(group_id, request)?;
        let resp = self.evaluate_command(shard_id, request).await?;
        Ok(GroupResponse::new(resp))
    }

    /// Change the configuration of raft group.
    pub async fn change_config(&self) {
        todo!()
    }

    pub async fn on_leader(&self) -> Result<()> {
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

    #[inline]
    pub fn replica_id(&self) -> u64 {
        self.replica_id
    }

    #[inline]
    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    #[inline]
    pub fn raft_node(&self) -> RaftNodeFacade {
        self.raft_node.clone()
    }
}

impl LeaseState {
    fn still_valid(&self) -> bool {
        use ::raft::StateRole;
        self.role == StateRole::Leader
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
            for waker in std::mem::take(&mut lease_state.leader_subscribers) {
                waker.wake();
            }
        }
    }
}
