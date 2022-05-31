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
mod raft;

use std::sync::Arc;

use engula_api::{
    server::v1::{
        group_request_union::Request, group_response_union::Response, GroupDesc, GroupRequest,
        GroupResponse, GroupResponseUnion,
    },
    v1::{DeleteResponse, GetResponse, PutResponse},
};
use futures::lock::Mutex;

use self::{
    fsm::GroupStateMachine,
    raft::{RaftNodeFacade, StateObserver},
};
use super::group_engine::GroupEngine;
use crate::{serverpb::v1::EvalResult, Error, Result};

#[derive(Default)]
struct LeaseState {
    role: ::raft::StateRole,
    term: u64,
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

#[allow(unused)]
impl Replica {
    /// Create new instance of the specified raft node.
    pub async fn create(
        replica_id: u64,
        group_engine: GroupEngine,
        target_desc: &GroupDesc,
    ) -> Result<()> {
        let replicas = target_desc
            .replicas
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>();
        let fsm = Box::new(GroupStateMachine::new(group_engine));
        RaftNodeFacade::create(replica_id, replicas, fsm).await?;
        Ok(())
    }

    /// Open the existed replica of raft group.
    pub async fn open(group_id: u64, replica_id: u64, group_engine: GroupEngine) -> Result<Self> {
        let fsm = Box::new(GroupStateMachine::new(group_engine.clone()));
        let lease_state: Arc<Mutex<LeaseState>> = Arc::default();
        let observer = Box::new(RoleObserver::new(lease_state.clone()));
        let raft_node = RaftNodeFacade::open(replica_id, fsm, observer).await?;
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
        let shard_id = group_request.shard_id;
        let request = group_request
            .request
            .as_ref()
            .and_then(|request| request.request.as_ref())
            .ok_or_else(|| Error::Invalid("GroupRequest".into()))?;

        self.check_request_early(request).await?;
        let resp = self.evaluate_command(shard_id, request).await?;
        Ok(GroupResponse {
            response: Some(GroupResponseUnion {
                response: Some(resp),
            }),
            status: None,
        })
    }

    /// Change the configuration of raft group.
    pub async fn change_config(&self) {
        todo!()
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
        };

        if let Some(eval_result) = eval_result_opt {
            self.propose(eval_result).await?;
        }

        Ok(resp)
    }

    async fn propose(&self, eval_result: EvalResult) -> Result<()> {
        self.raft_node.propose(eval_result).await?;
        Ok(())
    }

    async fn check_request_early(&self, _request: &Request) -> Result<()> {
        let lease_state = self.lease_state.lock().await;
        if !lease_state.still_valid() {
            Err(Error::InvalidLease)
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

#[tonic::async_trait]
impl StateObserver for RoleObserver {
    async fn on_state_updated(&mut self, role: ::raft::StateRole, term: u64) {
        let mut lease_state = self.lease_state.lock().await;
        lease_state.role = role;
        lease_state.term = term;
    }
}