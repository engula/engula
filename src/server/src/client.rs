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

use std::{collections::HashMap, future::Future, sync::Arc, task::Poll, time::Duration};

use engula_api::server::v1::{group_response_union::Response, *};
use engula_client::{NodeClient, RequestBatchBuilder};
use futures::{FutureExt, StreamExt};
use tonic::{Code, Status};
use tracing::warn;

use crate::{Error, Provider, Result};

pub struct RetryableShardChunkStreaming<'a> {
    shard_id: u64,
    last_key: Vec<u8>,
    client: &'a mut GroupClient,
    streaming: tonic::Streaming<ShardChunk>,
}

pub struct GroupClient {
    group_id: u64,

    /// Node id to node client.
    node_clients: HashMap<u64, NodeClient>,

    epoch: u64,
    leader_node_id: Option<u64>,
    replicas: Vec<ReplicaDesc>,
    next_access_index: usize,

    provider: Arc<Provider>,
}

impl GroupClient {
    pub(crate) fn new(group_id: u64, provider: Arc<Provider>) -> Self {
        GroupClient {
            group_id,

            node_clients: HashMap::default(),
            epoch: 0,
            leader_node_id: None,
            replicas: Vec::default(),
            next_access_index: 0,
            provider,
        }
    }

    async fn invoke<F, O, V>(&mut self, op: F) -> Result<V>
    where
        F: Fn(u64, u64, u64, NodeClient) -> O,
        O: Future<Output = std::result::Result<V, tonic::Status>>,
    {
        self.invoke_opt(op, false).await
    }

    async fn invoke_opt<F, O, V>(&mut self, op: F, accurate_epoch: bool) -> Result<V>
    where
        F: Fn(u64, u64, u64, NodeClient) -> O,
        O: Future<Output = std::result::Result<V, tonic::Status>>,
    {
        loop {
            let client = self.recommend_client().await;
            match op(
                self.group_id,
                self.epoch,
                self.leader_node_id.unwrap_or_default(),
                client,
            )
            .await
            {
                Ok(s) => return Ok(s),
                Err(status) => {
                    self.apply_status(status, accurate_epoch).await?;
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
        self.leader_node_id = None;
        if self.replicas.is_empty() {
            if let Ok(group) = self.provider.router.find_group(self.group_id) {
                self.epoch = group.epoch.unwrap_or_default();
                self.leader_node_id = group
                    .replicas
                    .get(&group.leader_id.unwrap_or_default())
                    .map(|r| r.node_id);
                self.replicas = group.replicas.into_iter().map(|(_, v)| v).collect();
                if self.leader_node_id.is_some() {
                    return self.leader_node_id;
                }
            }
        }

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

        if let Ok(addr) = self.provider.router.find_node_addr(node_id) {
            match self
                .provider
                .conn_manager
                .get_node_client(addr.clone())
                .await
            {
                Ok(client) => {
                    self.node_clients.insert(node_id, client.clone());
                    return Some(client);
                }
                Err(err) => {
                    warn!("connect to node {node_id} address {addr}: {err:?}");
                }
            }
        } else {
            warn!("not found the address of node {node_id}");
        }

        None
    }

    async fn apply_status(&mut self, status: tonic::Status, accurate_epoch: bool) -> Result<()> {
        match Error::from(status) {
            Error::GroupNotFound(_) => {
                self.leader_node_id = None;
                Ok(())
            }
            Error::NotLeader(_, replica_desc) => {
                self.leader_node_id = replica_desc.map(|r| r.node_id);
                Ok(())
            }
            // If the exact epoch is required, don't retry if epoch isn't matched.
            Error::EpochNotMatch(group_desc) if !accurate_epoch => {
                if group_desc.epoch > self.epoch {
                    self.replicas = group_desc.replicas;
                    self.epoch = group_desc.epoch;
                } else {
                    self.leader_node_id = None;
                }
                Ok(())
            }
            Error::Rpc(status) if status.code() == tonic::Code::Unavailable => {
                self.leader_node_id = None;
                Ok(())
            }
            e => Err(e),
        }
    }
}

impl GroupClient {
    pub async fn list(&mut self, shard_id: u64, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let req = RequestBatchBuilder::new(node_id)
                .shard_prefix(group_id, epoch, shard_id, prefix)
                .build();
            async move {
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::PrefixList(resp) => Ok(resp.values),
                    _ => Err(Status::internal(
                        "invalid response type, PrefixList is required",
                    )),
                }
            }
        };
        self.invoke(op).await
    }

    pub async fn create_shard(&mut self, desc: &ShardDesc) -> Result<()> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let desc = desc.to_owned();
            let req = RequestBatchBuilder::new(node_id)
                .create_shard(group_id, epoch, desc)
                .build();
            async move {
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::CreateShard(_) => Ok(()),
                    _ => Err(Status::internal(
                        "invalid response type, CreateShard is required",
                    )),
                }
            }
        };
        self.invoke(op).await
    }

    pub async fn transfer_leader(&mut self, dest_replica: u64) -> Result<()> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let dest_replica = dest_replica.to_owned();
            let req = RequestBatchBuilder::new(node_id)
                .transfer_leader(group_id, epoch, dest_replica)
                .build();
            async move {
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::Transfer(_) => Ok(()),
                    _ => Err(Status::internal(
                        "invalid response type, Transfer is required",
                    )),
                }
            }
        };
        self.invoke(op).await
    }

    pub async fn remove_group_replica(&mut self, remove_replica: u64) -> Result<()> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let remove_replica = remove_replica.to_owned();
            let req = RequestBatchBuilder::new(node_id)
                .remove_replica(group_id, epoch, remove_replica)
                .build();
            async move {
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::ChangeReplicas(_) => Ok(()),
                    _ => Err(Status::internal(
                        "invalid response type, ChangeReplicas is required",
                    )),
                }
            }
        };
        self.invoke(op).await
    }

    pub async fn add_replica(&mut self, replica: u64, node: u64) -> Result<()> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let req = RequestBatchBuilder::new(node_id)
                .add_replica(group_id, epoch, replica, node)
                .build();
            async move {
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::ChangeReplicas(_) => Ok(()),
                    _ => Err(Status::internal(
                        "invalid response type, ChangeReplicas is required",
                    )),
                }
            }
        };
        self.invoke(op).await
    }

    pub async fn add_learner(&mut self, replica: u64, node: u64) -> Result<()> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let req = RequestBatchBuilder::new(node_id)
                .add_learner(group_id, epoch, replica, node)
                .build();
            async move {
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::ChangeReplicas(_) => Ok(()),
                    _ => Err(Status::internal(
                        "invalid response type, ChangeReplicas is required",
                    )),
                }
            }
        };
        self.invoke(op).await
    }

    pub async fn migrate_shard(
        &mut self,
        src_group: u64,
        src_epoch: u64,
        shard: &ShardDesc,
    ) -> Result<()> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let req = RequestBatchBuilder::new(node_id)
                .accept_shard(group_id, epoch, src_group, src_epoch, shard)
                .build();
            async move {
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::AcceptShard(_) => Ok(()),
                    _ => Err(Status::internal(
                        "invalid response type, AcceptShard is required",
                    )),
                }
            }
        };
        self.invoke(op).await
    }

    fn batch_response<T>(mut resps: Vec<T>) -> std::result::Result<T, Status> {
        if resps.is_empty() {
            Err(Status::internal(
                "response of batch request is empty".to_owned(),
            ))
        } else {
            Ok(resps.pop().unwrap())
        }
    }

    fn group_response(resp: GroupResponse) -> std::result::Result<Response, Status> {
        use prost::Message;

        if let Some(resp) = resp.response.and_then(|resp| resp.response) {
            Ok(resp)
        } else if let Some(err) = resp.error {
            Err(Status::with_details(
                Code::Unknown,
                "response",
                err.encode_to_vec().into(),
            ))
        } else {
            Err(Status::internal(
                "Both response and error are None in GroupResponse".to_owned(),
            ))
        }
    }
}

// Migration related functions.
impl GroupClient {
    pub async fn setup_migration(&mut self, desc: &MigrationDesc) -> Result<MigrateResponse> {
        let op = |_, _, _, client: NodeClient| {
            let req = MigrateRequest {
                desc: Some(desc.clone()),
                action: MigrateAction::Setup as i32,
            };
            async move { client.migrate(req).await }
        };
        self.invoke_opt(op, /* accurate_epoch= */ true).await
    }

    pub async fn commit_migration(&mut self, desc: &MigrationDesc) -> Result<MigrateResponse> {
        let op = |_, _, _, client: NodeClient| {
            let req = MigrateRequest {
                desc: Some(desc.clone()),
                action: MigrateAction::Commit as i32,
            };
            async move { client.migrate(req).await }
        };
        self.invoke(op).await
    }

    pub async fn forward(&mut self, req: ForwardRequest) -> Result<ForwardResponse> {
        let op = |_, _, _, client: NodeClient| {
            let cloned_req = req.clone();
            async move { client.forward(cloned_req).await }
        };
        self.invoke(op).await
    }

    pub async fn retryable_pull(
        &mut self,
        shard_id: u64,
        last_key: Vec<u8>,
    ) -> Result<RetryableShardChunkStreaming> {
        let streaming = self.pull(shard_id, &last_key).await?;
        let retryable_streaming = RetryableShardChunkStreaming {
            shard_id,
            last_key,
            client: self,
            streaming,
        };
        Ok(retryable_streaming)
    }

    async fn pull(
        &mut self,
        shard_id: u64,
        last_key: &[u8],
    ) -> Result<tonic::Streaming<ShardChunk>> {
        let group_id = self.group_id;
        let op = |_, _, _, client: NodeClient| {
            let request = PullRequest {
                group_id,
                shard_id,
                last_key: last_key.to_owned(),
            };
            async move { client.pull(request).await }
        };
        self.invoke(op).await
    }
}

impl<'a> RetryableShardChunkStreaming<'a> {
    async fn next(&mut self) -> Option<Result<ShardChunk>> {
        loop {
            let item = match self.streaming.next().await {
                None => return None,
                Some(item) => item,
            };
            match item {
                Ok(item) => {
                    debug_assert!(!item.data.is_empty());
                    self.last_key = item.data.last().unwrap().key.clone();
                    return Some(Ok(item));
                }
                Err(status) => {
                    if let Err(e) = self.client.apply_status(status, false).await {
                        return Some(Err(e));
                    }
                }
            }

            // retry, by recreate new stream.
            match self.client.pull(self.shard_id, &self.last_key).await {
                Ok(streaming) => self.streaming = streaming,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl<'a> futures::Stream for RetryableShardChunkStreaming<'a> {
    type Item = Result<ShardChunk>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let future = self.get_mut().next();
        futures::pin_mut!(future);
        future.poll_unpin(cx)
    }
}
