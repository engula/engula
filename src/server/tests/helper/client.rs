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

use std::{collections::HashMap, future::Future, time::Duration};

use engula_api::server::v1::*;
use engula_client::NodeClient;
use engula_server::{runtime, Error, Result};
use prost::Message;
use tonic::{Code, Status};
use tracing::warn;

pub async fn node_client_with_retry(addr: &str) -> NodeClient {
    for _ in 0..10000 {
        match NodeClient::connect(addr.to_string()).await {
            Ok(client) => return client,
            Err(_) => {
                runtime::time::sleep(Duration::from_millis(3000)).await;
            }
        };
    }
    panic!("connect to {} timeout", addr);
}

#[allow(unused)]
pub struct GroupClient {
    /// Node id to node client.
    node_clients: HashMap<u64, NodeClient>,

    leader_node_id: Option<u64>,
    replicas: Vec<u64>,
    next_access_index: usize,

    nodes: HashMap<u64, String>,
}

#[allow(unused)]
impl GroupClient {
    pub fn new(nodes: HashMap<u64, String>) -> Self {
        GroupClient {
            node_clients: HashMap::default(),
            leader_node_id: None,
            replicas: nodes.keys().cloned().collect(),
            next_access_index: 0,

            nodes,
        }
    }

    pub async fn group(&mut self, req: GroupRequest) -> Result<GroupResponse> {
        let op = |client: NodeClient| {
            // FIXME(walter) support epoch not match
            let batch_req = BatchRequest {
                node_id: 0,
                requests: vec![req.clone()],
            };
            async move {
                let mut resps = client.batch_group_requests(batch_req).await?;
                let resp = resps.pop().unwrap();
                if resp.response.is_some() {
                    Ok(resp)
                } else {
                    Err(Status::with_details(
                        Code::Unknown,
                        "unknown",
                        resp.error.unwrap().encode_to_vec().into(),
                    ))
                }
            }
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
            let node_id = self.replicas[self.next_access_index];
            self.next_access_index = (self.next_access_index + 1) % self.replicas.len();
            Some(node_id)
        } else {
            None
        }
    }

    async fn fetch_client(&mut self, node_id: u64) -> Option<NodeClient> {
        if let Some(client) = self.node_clients.get(&node_id) {
            return Some(client.clone());
        }

        if let Some(addr) = self.nodes.get(&node_id) {
            match NodeClient::connect(addr.clone()).await {
                Ok(client) => {
                    self.node_clients.insert(node_id, client.clone());
                    return Some(client);
                }
                Err(err) => {
                    warn!("connect to node {} address {}: {}", node_id, addr, err);
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
            Error::EpochNotMatch(_) => {
                self.leader_node_id = None;
                Ok(())
            }
            e => Err(e),
        }
    }
}
