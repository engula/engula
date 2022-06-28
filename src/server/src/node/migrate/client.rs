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

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use engula_api::server::v1::*;
use engula_client::NodeClient;
use tracing::warn;

use crate::{raftgroup::AddressResolver, Error, Result};

pub struct GroupClient {
    group_id: u64,

    /// Node id to node client.
    node_clients: HashMap<u64, NodeClient>,

    /// Specific the expected epoch of request. Don't retry if epoch isn't matched.
    expect_epoch: Option<u64>,

    leader_node_id: Option<u64>,
    replicas: Vec<ReplicaDesc>,
    next_access_index: usize,

    address_resolver: Arc<dyn AddressResolver>,
}

impl GroupClient {
    pub fn new(
        group_id: u64,
        expect_epoch: Option<u64>,
        address_resolver: Arc<dyn AddressResolver>,
    ) -> Self {
        GroupClient {
            group_id,

            node_clients: HashMap::default(),
            expect_epoch,
            leader_node_id: None,
            replicas: Vec::default(),
            next_access_index: 0,

            address_resolver,
        }
    }

    pub async fn migrate(&mut self, req: MigrateRequest) -> Result<MigrateResponse> {
        let op = |client: NodeClient| {
            let cloned_req = req.clone();
            async move { client.migrate(cloned_req).await }
        };
        self.invoke(op).await
    }

    pub async fn forward(&mut self, req: ForwardRequest) -> Result<ForwardResponse> {
        let op = |client: NodeClient| {
            let cloned_req = req.clone();
            async move { client.forward(cloned_req).await }
        };
        self.invoke(op).await
    }

    pub async fn pull(
        &mut self,
        shard_id: u64,
        last_key: &[u8],
    ) -> Result<tonic::Streaming<ShardChunk>> {
        let group_id = self.group_id;
        let op = |client: NodeClient| {
            let request = PullRequest {
                group_id,
                shard_id,
                last_key: last_key.to_owned(),
            };
            async move { client.pull(request).await }
        };
        self.invoke(op).await
    }

    async fn invoke<F, O, V>(&mut self, op: F) -> Result<V>
    where
        F: Fn(NodeClient) -> O,
        O: Future<Output = std::result::Result<V, tonic::Status>>,
    {
        loop {
            let client = self.recommend_client().await;
            match op(client).await {
                Ok(s) => return Ok(s),
                Err(status) => {
                    self.apply_status(status).await?;
                }
            }
        }
    }

    async fn recommend_client(&mut self) -> NodeClient {
        let mut interval = 1;
        loop {
            let recommend_node_id = self.leader_node_id.or_else(|| self.next_access_node_id());

            if let Some(node_id) = recommend_node_id {
                if let Some(client) = self.fetch_client(node_id).await {
                    // Pretend that the current node is the leader. If this request is successful,
                    // subsequent requests can directly use it as the leader, otherwise it will be
                    // reset in `apply_status`.
                    if self.leader_node_id.is_none() {
                        self.leader_node_id = Some(node_id);
                    }
                    return client;
                }
            }

            tokio::time::sleep(Duration::from_millis(interval)).await;
            interval = std::cmp::max(interval * 2, 1000);
        }
    }

    fn next_access_node_id(&mut self) -> Option<u64> {
        if !self.replicas.is_empty() {
            let replica_desc = &self.replicas[self.next_access_index];
            self.next_access_index = (self.next_access_index + 1) % self.replicas.len();
            Some(replica_desc.node_id)
        } else {
            None
        }
    }

    async fn fetch_client(&mut self, node_id: u64) -> Option<NodeClient> {
        if let Some(client) = self.node_clients.get(&node_id) {
            return Some(client.clone());
        }

        if let Ok(node_desc) = self.address_resolver.resolve(node_id).await {
            match NodeClient::connect(node_desc.addr.clone()).await {
                Ok(client) => {
                    self.node_clients.insert(node_id, client.clone());
                    return Some(client);
                }
                Err(err) => {
                    warn!(
                        "connect to node {} address {}: {}",
                        node_id, node_desc.addr, err
                    );
                }
            }
        } else {
            warn!("not found the address of node {}", node_id);
        }

        None
    }

    async fn apply_status(&mut self, status: tonic::Status) -> Result<()> {
        match Error::from(status) {
            Error::GroupNotFound(_) => {
                self.leader_node_id = None;
                Ok(())
            }
            Error::NotLeader(_, replica_desc) => {
                self.leader_node_id = replica_desc.map(|r| r.node_id);
                Ok(())
            }
            Error::EpochNotMatch(_) if self.expect_epoch.is_none() => {
                self.leader_node_id = None;
                Ok(())
            }
            e => Err(e),
        }
    }
}
