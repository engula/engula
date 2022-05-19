// Copyright 2022 Engula Contributors
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

use tonic::transport::Channel;

use crate::{
    master_client, JoinMemberRequest, LeaveMemberRequest, LookupMemberRequest, MemberId,
    NodeDescriptor,
};

#[derive(Debug, Clone)]
pub struct Client {
    client: master_client::MasterClient<Channel>,
}

impl Client {
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let addr = format!("http://{}", addr);
        let client = master_client::MasterClient::connect(addr).await?;
        Ok(Self { client })
    }

    pub async fn join_member(
        &self,
        descs: Vec<NodeDescriptor>,
    ) -> Result<Vec<MemberId>, tonic::Status> {
        let mut client = self.client.clone();
        let req = JoinMemberRequest { desc: descs };
        let res = client.join_member(req).await?;
        Ok(res.into_inner().id)
    }

    pub async fn leave_member(&self, ids: Vec<MemberId>) -> Result<Vec<MemberId>, tonic::Status> {
        let mut client = self.client.clone();
        let req = LeaveMemberRequest { id: ids };
        let res = client.leave_member(req).await?;
        Ok(res.into_inner().id)
    }

    pub async fn lookup_member(
        &self,
        ids: Vec<MemberId>,
    ) -> Result<Vec<NodeDescriptor>, tonic::Status> {
        let mut client = self.client.clone();
        let req = LookupMemberRequest { id: ids };
        let res = client.lookup_member(req).await?;
        Ok(res.into_inner().desc)
    }
}
