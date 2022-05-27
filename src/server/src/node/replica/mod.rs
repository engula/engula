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
mod fsm;
mod raft;

use engula_api::server::v1::{GroupRequest, GroupResponse};

use self::raft::RaftNodeFacade;
use super::group_engine::GroupEngine;
use crate::{Error, Result};

#[allow(unused)]
pub struct Replica
where
    Self: Send,
{
    group_engine: GroupEngine,
    raft_node: RaftNodeFacade,
}

#[allow(unused)]
impl Replica {
    /// Execute group request and fill response.
    pub async fn execute(&self, group_request: GroupRequest) -> Result<GroupResponse> {
        todo!()
    }

    /// Change the configuration of raft group.
    pub async fn change_config(&self) {
        todo!()
    }

    async fn request_to_proposal(&self) {
        todo!()
    }

    async fn evaluate_proposal(&self) {
        todo!()
    }

    /// Delegates the eval method for the given `GroupRequest`.
    async fn evaluate_command(&self, group_request: &GroupRequest) -> Result<GroupResponse> {
        use engula_api::server::v1::group_request_union::Request;
        let request = group_request
            .request
            .as_ref()
            .ok_or_else(|| Error::Invalid("GroupRequest".into()))?;
        match &request.request {
            Some(Request::Get(req)) => {}
            Some(Request::Put(req)) => {}
            Some(Request::Delete(req)) => {}
            None => {}
        }
        todo!()
    }

    async fn propose(&self) {
        todo!()
    }
}
