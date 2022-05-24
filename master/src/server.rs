// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tonic::{Request, Response, Status};

use crate::{
    master::Master,
    proto::{
        master_server, JoinMemberRequest, JoinMemberResponse, LeaveMemberRequest,
        LeaveMemberResponse, LookupMemberRequest, LookupMemberResponse,
    },
};

#[derive(Debug, Clone)]
pub struct Server {
    master: Master,
}

impl Server {
    pub fn new(master: Master) -> Self {
        Self { master }
    }

    pub fn into_service(self) -> master_server::MasterServer<Self> {
        master_server::MasterServer::new(self)
    }
}

#[tonic::async_trait]
impl master_server::Master for Server {
    async fn lookup_member(
        &self,
        req: Request<LookupMemberRequest>,
    ) -> Result<Response<LookupMemberResponse>, Status> {
        let req = req.into_inner();
        let mut res = LookupMemberResponse::default();
        for id in req.id.iter() {
            if let Some(desc) = self.master.lookup_member(id) {
                res.desc.push(desc);
            }
        }
        Ok(Response::new(res))
    }

    async fn join_member(
        &self,
        req: Request<JoinMemberRequest>,
    ) -> Result<Response<JoinMemberResponse>, Status> {
        let req = req.into_inner();
        let mut res = JoinMemberResponse::default();
        for desc in req.desc.iter() {
            let id = self.master.join_member(desc.clone());
            res.id.push(id);
        }
        Ok(Response::new(res))
    }

    async fn leave_member(
        &self,
        req: Request<LeaveMemberRequest>,
    ) -> Result<Response<LeaveMemberResponse>, Status> {
        let req = req.into_inner();
        let mut res = LeaveMemberResponse::default();
        for id in req.id.iter() {
            if self.master.leave_member(id) {
                res.id.push(id.clone());
            }
        }
        Ok(Response::new(res))
    }
}
