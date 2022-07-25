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

use std::{collections::HashMap, future::Future, io::ErrorKind, task::Poll, time::Duration};

use engula_api::{
    server::v1::{group_request_union::Request, group_response_union::Response, *},
    shard,
};
use futures::{FutureExt, StreamExt};
use tonic::{Code, Status};
use tracing::{debug, trace, warn};

use crate::{ConnManager, Error, NodeClient, RequestBatchBuilder, Result, Router};

pub struct RetryableShardChunkStreaming<'a> {
    shard_id: u64,
    last_key: Vec<u8>,
    client: &'a mut GroupClient,
    streaming: tonic::Streaming<ShardChunk>,
}

#[derive(Clone, Debug, Default)]
struct InvokeOpt<'a> {
    request: Option<&'a Request>,
    accurate_epoch: bool,
}

#[derive(Clone)]
pub struct GroupClient {
    group_id: u64,
    router: Router,
    conn_manager: ConnManager,

    epoch: u64,
    leader_state: Option<(u64, u64)>,
    replicas: Vec<ReplicaDesc>,

    // Cache the access node id to avoid polling again.
    access_node_id: Option<u64>,
    next_access_index: usize,

    /// Node id to node client.
    node_clients: HashMap<u64, NodeClient>,
}

impl GroupClient {
    pub fn new(group_id: u64, router: Router, conn_manager: ConnManager) -> Self {
        GroupClient {
            group_id,

            node_clients: HashMap::default(),
            epoch: 0,
            leader_state: None,
            access_node_id: None,
            replicas: Vec::default(),
            next_access_index: 0,

            router,
            conn_manager,
        }
    }

    async fn invoke<F, O, V>(&mut self, op: F) -> Result<V>
    where
        F: Fn(u64, u64, u64, NodeClient) -> O,
        O: Future<Output = std::result::Result<V, tonic::Status>>,
    {
        self.invoke_opt(op, InvokeOpt::default()).await
    }

    async fn invoke_opt<F, O, V>(&mut self, op: F, opt: InvokeOpt<'_>) -> Result<V>
    where
        F: Fn(u64, u64, u64, NodeClient) -> O,
        O: Future<Output = std::result::Result<V, tonic::Status>>,
    {
        // FIXME(walter) support timeout
        for _ in 0..4 {
            let (node_id, client) = self.recommend_client().await;
            trace!("issue rpc request to node {node_id}");
            match op(self.group_id, self.epoch, node_id, client).await {
                Ok(s) => return Ok(s),
                Err(status) => {
                    self.apply_status(status, &opt).await?;
                }
            }
        }

        // FIXME(walter) support timeout
        Err(Error::EpochNotMatch(GroupDesc::default()))
    }

    async fn recommend_client(&mut self) -> (u64, NodeClient) {
        let mut interval = 1;
        loop {
            if self.replicas.len() <= self.next_access_index {
                self.refresh_group_state();
            }

            let recommend_node_id = self.access_node_id.or_else(|| self.next_access_node_id());
            if let Some(node_id) = recommend_node_id {
                if let Some(client) = self.fetch_client(node_id).await {
                    self.access_node_id = Some(node_id);
                    return (node_id, client);
                }
            }

            self.access_node_id = None;

            // Sleep before the next round to avoid busy requests.
            tokio::time::sleep(Duration::from_millis(interval)).await;
            interval = std::cmp::max(interval * 2, 1000);
        }
    }

    fn refresh_group_state(&mut self) {
        self.next_access_index = 0;
        if let Ok(group) = self.router.find_group(self.group_id) {
            if self.epoch < group.epoch {
                let mut leader_node_id = None;
                if let Some((leader_id, _)) = group.leader_state {
                    if let Some(desc) = group.replicas.get(&leader_id) {
                        leader_node_id = Some(desc.node_id);
                    }
                };
                self.leader_state = group.leader_state;
                self.epoch = group.epoch;
                self.replicas = group.replicas.into_iter().map(|(_, v)| v).collect();
                if let Some(node_id) = leader_node_id {
                    trace!(
                        "group client refresh group {} state with leader node id {}",
                        self.group_id,
                        node_id
                    );
                    move_node_to_first_element(&mut self.replicas, node_id);
                }
            }
        }
    }

    /// Return the next node id, skip the leader node.
    fn next_access_node_id(&mut self) -> Option<u64> {
        if self.next_access_index < self.replicas.len() {
            let replica_desc = &self.replicas[self.next_access_index];
            self.next_access_index += 1;
            Some(replica_desc.node_id)
        } else {
            None
        }
    }

    async fn fetch_client(&mut self, node_id: u64) -> Option<NodeClient> {
        if let Some(client) = self.node_clients.get(&node_id) {
            return Some(client.clone());
        }

        if let Ok(addr) = self.router.find_node_addr(node_id) {
            match self.conn_manager.get_node_client(addr.clone()).await {
                Ok(client) => {
                    trace!("connect node {node_id} with addr {addr}");
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

    async fn apply_status(&mut self, status: tonic::Status, opt: &InvokeOpt<'_>) -> Result<()> {
        match Error::from(status) {
            Error::GroupNotFound(_) => {
                debug!(
                    "group client issue rpc to {}: group {} not found",
                    self.access_node_id.unwrap_or_default(),
                    self.group_id
                );
                self.access_node_id = None;
                Ok(())
            }
            Error::NotLeader(_, term, leader_desc) => {
                debug!(
                    "group client issue rpc to {}: not leader of group {}, new leader {:?}",
                    self.access_node_id.unwrap_or_default(),
                    self.group_id,
                    leader_desc
                );
                self.access_node_id = None;
                if let Some(leader) = leader_desc {
                    // Ignore staled `NotLeader` response.
                    if !self
                        .leader_state
                        .map(|(_, local_term)| local_term >= term)
                        .unwrap_or_default()
                    {
                        move_node_to_first_element(&mut self.replicas, leader.node_id);
                        self.access_node_id = Some(leader.node_id);
                        self.leader_state = Some((leader.id, term));
                    }
                }
                Ok(())
            }
            Error::Rpc(status) if retryable_rpc_err(&status) => {
                debug!(
                    "group client issue rpc to {}: group {} with retryable status: {}",
                    self.access_node_id.unwrap_or_default(),
                    self.group_id,
                    status.to_string(),
                );
                self.access_node_id = None;
                Ok(())
            }
            // If the exact epoch is required, don't retry if epoch isn't matched.
            Error::EpochNotMatch(group_desc) if !opt.accurate_epoch => {
                if group_desc.epoch <= self.epoch {
                    panic!(
                        "receive EpochNotMatch, but local epoch {} is not less than remote: {:?}",
                        self.epoch, group_desc
                    );
                }

                debug!(
                    "group client issue rpc to {}: group {} epoch {} not match target epoch {}",
                    self.access_node_id.unwrap_or_default(),
                    self.group_id,
                    self.epoch,
                    group_desc.epoch,
                );

                if opt
                    .request
                    .map(|r| !is_executable(&group_desc, r))
                    .unwrap_or_default()
                {
                    // The target group would not execute the specified request.
                    Err(Error::EpochNotMatch(group_desc))
                } else {
                    self.replicas = group_desc.replicas;
                    self.epoch = group_desc.epoch;
                    self.next_access_index = 1;
                    move_node_to_first_element(
                        &mut self.replicas,
                        self.access_node_id.unwrap_or_default(),
                    );
                    Ok(())
                }
            }
            e => {
                // FIXME(walter) performance
                warn!(err = ?e, "group client issue rpc");
                Err(e)
            }
        }
    }
}

impl GroupClient {
    pub async fn request(&mut self, request: &Request) -> Result<Response> {
        let op = |group_id, epoch, node_id, client: NodeClient| {
            let req = BatchRequest {
                node_id,
                requests: vec![GroupRequest {
                    group_id,
                    epoch,
                    request: Some(GroupRequestUnion {
                        request: Some(request.clone()),
                    }),
                }],
            };
            async move {
                client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)
            }
        };

        let opt = InvokeOpt {
            request: Some(request),
            accurate_epoch: false,
        };
        self.invoke_opt(op, opt).await
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

    pub async fn accept_shard(
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
        let opt = InvokeOpt {
            accurate_epoch: true,
            ..Default::default()
        };
        self.invoke_opt(op, opt).await
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
                    if let Err(e) = self
                        .client
                        .apply_status(status, &InvokeOpt::default())
                        .await
                    {
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

fn retryable_rpc_err(status: &tonic::Status) -> bool {
    if status.code() == tonic::Code::Unavailable {
        return true;
    }
    if status.code() == tonic::Code::Unknown {
        if let Some(err) = find_source::<std::io::Error>(status) {
            return retryable_io_err(err);
        }
    }
    false
}

fn find_source<E: std::error::Error + 'static>(err: &tonic::Status) -> Option<&E> {
    use std::error::Error;
    let mut cause = err.source();
    while let Some(err) = cause {
        if let Some(typed) = err.downcast_ref() {
            return Some(typed);
        }
        cause = err.source();
    }
    None
}

fn retryable_io_err(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
    )
}

fn is_executable(descriptor: &GroupDesc, request: &Request) -> bool {
    match request {
        Request::Get(req) => {
            is_target_shard_exists(descriptor, req.shard_id, &req.get.as_ref().unwrap().key)
        }
        Request::Put(req) => {
            is_target_shard_exists(descriptor, req.shard_id, &req.put.as_ref().unwrap().key)
        }
        Request::Delete(req) => {
            is_target_shard_exists(descriptor, req.shard_id, &req.delete.as_ref().unwrap().key)
        }
        Request::PrefixList(req) => is_target_shard_exists(descriptor, req.shard_id, &req.prefix),
        _ => false,
    }
}

fn is_target_shard_exists(desc: &GroupDesc, shard_id: u64, key: &[u8]) -> bool {
    // TODO(walter) support migrate meta.
    desc.shards
        .iter()
        .find(|s| s.id == shard_id)
        .map(|s| shard::belong_to(s, key))
        .unwrap_or_default()
}

fn move_node_to_first_element(replicas: &mut [ReplicaDesc], node_id: u64) {
    if let Some(idx) = replicas
        .iter()
        .position(|replica| replica.node_id == node_id)
    {
        if idx != 0 {
            replicas.swap(0, idx)
        }
    }
}
