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
use tonic::{transport::Channel, Streaming};

use crate::NodeClient;

#[derive(Debug, Clone)]
pub struct Client {
    client: root_client::RootClient<Channel>,
}

impl Client {
    pub async fn connect(addr: String) -> Result<Self, crate::Error> {
        let node_client = NodeClient::connect(addr).await?;
        let root_addrs = node_client.get_root().await?;
        let mut errs = vec![];
        for root_addr in root_addrs {
            let root_addr = format!("http://{}", root_addr);
            match root_client::RootClient::connect(root_addr).await {
                Ok(client) => return Ok(Self { client }),
                Err(err) => errs.push(err),
            }
        }
        Err(crate::Error::MultiTransport(errs))
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

    pub async fn watch(&self, sequence: u64) -> Result<Streaming<WatchResponse>, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.watch(WatchRequest { sequence }).await?;
        Ok(res.into_inner())
    }
}
