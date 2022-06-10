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

use engula_api::{server::v1::*, v1::*};
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct Client {
    client: root_client::RootClient<Channel>,
}

impl Client {
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let addr = format!("http://{}", addr);
        let client = root_client::RootClient::connect(addr).await?;
        Ok(Self { client })
    }

    // TODO improve building admin request
    pub async fn admin(&self, req: AdminRequest) -> Result<AdminResponse, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.admin(req).await?;
        Ok(res.into_inner())
    }

    pub async fn join_node(&self, req: JoinNodeRequest) -> Result<JoinNodeResponse, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.join(req).await?;
        Ok(res.into_inner())
    }

    // TODO removed once `watch` implemented
    pub async fn resolve(&self, node_id: u64) -> Result<Option<NodeDesc>, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.resolve(ResolveNodeRequest { node_id }).await?;
        Ok(res.into_inner().node)
    }
}
