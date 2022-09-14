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

use std::{
    collections::HashMap,
    future::Future,
    task::Poll,
    time::{Duration, Instant},
};

use engula_api::{
    server::v1::{self, group_request_union::Request, group_response_union::Response, *},
    shard,
};
use futures::{FutureExt, StreamExt};
use tonic::{Code, Status};
use tracing::{debug, trace, warn};

use crate::{
    metrics::*, node_client::RpcTimeout, record_latency_opt, ConnManager, Error, NodeClient,
    RequestBatchBuilder, Result, Router, RouterGroupState,
};

pub struct RetryableShardChunkStreaming {
    shard_id: u64,
    last_key: Vec<u8>,
    client: GroupClient,
    streaming: tonic::Streaming<ShardChunk>,
}

#[derive(Clone, Debug, Default)]
struct InvokeOpt<'a> {
    request: Option<&'a Request>,

    /// It indicates that the value of epoch is accurate. If `EpochNotMatch` is encountered, it
    /// means that the precondition is not satisfied, and there is no need to retry.
    accurate_epoch: bool,

    /// It points out that the associated request is idempotent, and if a transport error
    /// (connection reset, broken pipe) is encountered, it can be retried safety.
    ignore_transport_error: bool,
}

#[derive(Clone, Debug, Default)]
struct InvokeContext {
    group_id: u64,
    epoch: u64,
    node_id: u64,
    timeout: Option<Duration>,
}

/// GroupClient is an abstraction for submitting requests to the leader of a group of replicas.
///
/// It provides leader positioning, automatic error retry (for retryable errors) and requests
/// timeout.
///
/// Of course, if it has traversed all the replicas and has not successfully submitted the
/// request, it will return `GroupNotAccessable`.
#[derive(Clone)]
pub struct GroupClient {
    group_id: u64,
    router: Router,
    conn_manager: ConnManager,
    timeout: Option<Duration>,

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
    pub fn lazy(group_id: u64, router: Router, conn_manager: ConnManager) -> Self {
        GroupClient {
            group_id,
            timeout: None,

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

    pub fn new(group_state: RouterGroupState, router: Router, conn_manager: ConnManager) -> Self {
        debug_assert!(!group_state.replicas.is_empty());
        let mut c = GroupClient::lazy(group_state.id, router, conn_manager);
        c.apply_group_state(group_state);
        c
    }

    /// Apply a timeout to next request issued via this client.
    ///
    /// NOTES: it depends the underlying request metadata (grpc-timeout header).
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout);
    }

    async fn invoke<F, O, V>(&mut self, op: F) -> Result<V>
    where
        F: Fn(InvokeContext, NodeClient) -> O,
        O: Future<Output = std::result::Result<V, tonic::Status>>,
    {
        self.invoke_with_opt(op, InvokeOpt::default()).await
    }

    async fn invoke_with_opt<F, O, V>(&mut self, op: F, opt: InvokeOpt<'_>) -> Result<V>
    where
        F: Fn(InvokeContext, NodeClient) -> O,
        O: Future<Output = std::result::Result<V, tonic::Status>>,
    {
        // Initial lazy connection
        if self.epoch == 0 {
            self.initial_group_state()?;
        }
        self.next_access_index = 0;

        let deadline = self
            .timeout
            .take()
            .map(|duration| Instant::now() + duration);
        let mut index = 0;
        let group_id = self.group_id;
        while let Some((node_id, client)) = self.recommend_client() {
            trace!("group {group_id} issue rpc request with index {index} to node {node_id}");
            index += 1;
            let ctx = InvokeContext {
                group_id,
                epoch: self.epoch,
                node_id,
                timeout: self.timeout,
            };
            match op(ctx, client).await {
                Err(status) => self.apply_status(status, &opt)?,
                Ok(s) => return Ok(s),
            };
            if deadline
                .map(|v| v.elapsed() > Duration::ZERO)
                .unwrap_or_default()
            {
                return Err(Error::DeadlineExceeded("issue rpc".to_owned()));
            }
            GROUP_CLIENT_RETRY_TOTAL.inc();
        }

        Err(Error::GroupNotAccessable(group_id))
    }

    fn recommend_client(&mut self) -> Option<(u64, NodeClient)> {
        while let Some(node_id) = self.access_node_id.or_else(|| self.next_access_node_id()) {
            if let Some(client) = self.fetch_client(node_id) {
                self.access_node_id = Some(node_id);
                return Some((node_id, client));
            }
            self.access_node_id = None;
        }
        None
    }

    fn initial_group_state(&mut self) -> Result<()> {
        debug_assert_eq!(self.epoch, 0);
        debug_assert!(self.replicas.is_empty());
        let group_state = self
            .router
            .find_group(self.group_id)
            .map_err(|_| Error::GroupNotAccessable(self.group_id))?;
        self.apply_group_state(group_state);
        Ok(())
    }

    pub fn apply_group_state(&mut self, group: RouterGroupState) {
        let leader_node_id = group
            .leader_state
            .and_then(|(leader_id, _)| group.replicas.get(&leader_id))
            .map(|desc| desc.node_id);

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

    /// Return the next node id, skip the leader node.
    fn next_access_node_id(&mut self) -> Option<u64> {
        // The first node is the current leader in most cases, making sure it retries more than
        // other nodes.
        if self.next_access_index <= self.replicas.len() {
            let replica_desc = &self.replicas[self.next_access_index % self.replicas.len()];
            self.next_access_index += 1;
            Some(replica_desc.node_id)
        } else {
            None
        }
    }

    fn fetch_client(&mut self, node_id: u64) -> Option<NodeClient> {
        if let Some(client) = self.node_clients.get(&node_id) {
            return Some(client.clone());
        }

        if let Ok(addr) = self.router.find_node_addr(node_id) {
            match self.conn_manager.get_node_client(addr.clone()) {
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

    fn apply_status(&mut self, status: tonic::Status, opt: &InvokeOpt<'_>) -> Result<()> {
        match Error::from(status) {
            Error::GroupNotFound(_) => {
                debug!(
                    "group {} issue rpc to {}: group not found",
                    self.group_id,
                    self.access_node_id.unwrap_or_default(),
                );
                self.access_node_id = None;
                Ok(())
            }
            Error::NotLeader(_, term, leader_desc) => {
                self.apply_not_leader_status(term, leader_desc);
                Ok(())
            }
            Error::Connect(status) => {
                debug!(
                    "group {} issue rpc to {}: with retryable status: {}",
                    self.group_id,
                    self.access_node_id.unwrap_or_default(),
                    status.to_string(),
                );
                self.access_node_id = None;
                Ok(())
            }
            Error::Transport(status)
                if opt.ignore_transport_error
                    || opt.request.map(is_read_only_request).unwrap_or_default() =>
            {
                debug!(
                    "group {} issue rpc to {}: with transport status: {}",
                    self.group_id,
                    self.access_node_id.unwrap_or_default(),
                    status.to_string(),
                );
                self.access_node_id = None;
                Ok(())
            }
            // If the exact epoch is required, don't retry if epoch isn't matched.
            Error::EpochNotMatch(group_desc) if !opt.accurate_epoch => {
                self.apply_epoch_not_match_status(group_desc, opt)
            }
            e => {
                warn!(
                    "group {} issue rpc to {}: epoch {} with unknown error {e:?}",
                    self.group_id,
                    self.access_node_id.unwrap_or_default(),
                    self.epoch,
                );
                Err(e)
            }
        }
    }

    fn apply_not_leader_status(&mut self, term: u64, leader_desc: Option<ReplicaDesc>) {
        debug!(
            "group {} issue rpc to {}: not leader, new leader {:?} term {term}, local state {:?}",
            self.group_id,
            self.access_node_id.unwrap_or_default(),
            leader_desc,
            self.leader_state,
        );
        self.access_node_id = None;
        if let Some(leader) = leader_desc {
            // Ignore staled `NotLeader` response.
            if !self
                .leader_state
                .map(|(_, local_term)| local_term >= term)
                .unwrap_or_default()
            {
                self.access_node_id = Some(leader.node_id);
                self.leader_state = Some((leader.id, term));

                // It is possible that the leader is not in the replica descs (because a staled
                // group descriptor is used). In order to ensure that the leader can be retried
                // later, the leader needs to be saved to the replicas.
                move_replica_to_first_element(&mut self.replicas, leader);
            }
        }
    }

    fn apply_epoch_not_match_status(
        &mut self,
        group_desc: GroupDesc,
        opt: &InvokeOpt<'_>,
    ) -> Result<()> {
        if group_desc.epoch <= self.epoch {
            panic!(
                "group {} receive EpochNotMatch, but local epoch {} is not less than remote: {:?}",
                self.group_id, self.epoch, group_desc
            );
        }

        debug!(
            "group {} issue rpc to {}: epoch {} not match target epoch {}",
            self.group_id,
            self.access_node_id.unwrap_or_default(),
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
            move_node_to_first_element(&mut self.replicas, self.access_node_id.unwrap_or_default());
            Ok(())
        }
    }
}

impl GroupClient {
    pub async fn request(&mut self, request: &Request) -> Result<Response> {
        let op = |ctx: InvokeContext, client: NodeClient| {
            let latency = take_group_request_metrics(request);
            let req = BatchRequest {
                node_id: ctx.node_id,
                requests: vec![GroupRequest {
                    group_id: ctx.group_id,
                    epoch: ctx.epoch,
                    request: Some(GroupRequestUnion {
                        request: Some(request.clone()),
                    }),
                }],
            };
            async move {
                record_latency_opt!(latency);
                client
                    .batch_group_requests(RpcTimeout::new(ctx.timeout, req))
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)
            }
        };

        let opt = InvokeOpt {
            request: Some(request),
            accurate_epoch: false,
            ignore_transport_error: false,
        };
        self.invoke_with_opt(op, opt).await
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

// Scheduling related functions that return GroupNotAccessable will be retried safely.
impl GroupClient {
    pub async fn create_shard(&mut self, desc: &ShardDesc) -> Result<()> {
        let op = |ctx: InvokeContext, client: NodeClient| {
            let desc = desc.to_owned();
            let req = RequestBatchBuilder::new(ctx.node_id)
                .create_shard(ctx.group_id, ctx.epoch, desc)
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
        let op = |ctx: InvokeContext, client: NodeClient| {
            let dest_replica = dest_replica.to_owned();
            let req = RequestBatchBuilder::new(ctx.node_id)
                .transfer_leader(ctx.group_id, ctx.epoch, dest_replica)
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
        let opt = InvokeOpt {
            accurate_epoch: true,
            ignore_transport_error: true,
            ..Default::default()
        };
        self.invoke_with_opt(op, opt).await
    }

    pub async fn remove_group_replica(&mut self, remove_replica: u64) -> Result<()> {
        let op = |ctx: InvokeContext, client: NodeClient| {
            let remove_replica = remove_replica.to_owned();
            let req = RequestBatchBuilder::new(ctx.node_id)
                .remove_replica(ctx.group_id, ctx.epoch, remove_replica)
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
        let op = |ctx: InvokeContext, client: NodeClient| {
            let req = RequestBatchBuilder::new(ctx.node_id)
                .add_replica(ctx.group_id, ctx.epoch, replica, node)
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

    pub async fn move_replicas(
        &mut self,
        incoming_voters: Vec<ReplicaDesc>,
        outgoing_voters: Vec<ReplicaDesc>,
    ) -> Result<ScheduleState> {
        let op = |ctx: InvokeContext, client: NodeClient| {
            let incoming_voters = incoming_voters.to_owned();
            let outgoing_voters = outgoing_voters.to_owned();
            let move_leader = outgoing_voters.iter().any(|r| r.node_id == ctx.node_id);
            let req = RequestBatchBuilder::new(ctx.node_id)
                .move_replica(ctx.group_id, ctx.epoch, incoming_voters, outgoing_voters)
                .build();
            async move {
                if move_leader {
                    use prost::Message;
                    let fake_error_to_retry = v1::Error::not_match(GroupDesc {
                        id: ctx.group_id,
                        epoch: 0,
                        shards: vec![],
                        replicas: vec![],
                    });
                    return Err(Status::with_details(
                        Code::Unknown,
                        "fake retry error",
                        fake_error_to_retry.encode_to_vec().into(),
                    ));
                }
                let resp = client
                    .batch_group_requests(req)
                    .await
                    .and_then(Self::batch_response)
                    .and_then(Self::group_response)?;
                match resp {
                    Response::MoveReplicas(resp) => Ok(resp),
                    _ => Err(Status::internal(
                        "invalid response type, MoveReplicas is required",
                    )),
                }
            }
        };
        let opt = InvokeOpt {
            accurate_epoch: true,
            ignore_transport_error: true,
            ..Default::default()
        };
        let resp = self.invoke_with_opt(op, opt).await?;
        resp.schedule_state.ok_or_else(|| {
            Error::Internal("invalid response type, `schedule_state` is required".into())
        })
    }

    pub async fn add_learner(&mut self, replica: u64, node: u64) -> Result<()> {
        let op = |ctx: InvokeContext, client: NodeClient| {
            let req = RequestBatchBuilder::new(ctx.node_id)
                .add_learner(ctx.group_id, ctx.epoch, replica, node)
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
        let op = |ctx: InvokeContext, client: NodeClient| {
            let req = RequestBatchBuilder::new(ctx.node_id)
                .accept_shard(ctx.group_id, ctx.epoch, src_group, src_epoch, shard)
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
        let opt = InvokeOpt {
            accurate_epoch: true,
            ignore_transport_error: true,
            ..Default::default()
        };
        self.invoke_with_opt(op, opt).await
    }
}

// Migration related functions, which will be retried at:
// `engula-client::migrate_client::MigrateClient`.
impl GroupClient {
    pub async fn setup_migration(&mut self, desc: &MigrationDesc) -> Result<MigrateResponse> {
        let op = |_: InvokeContext, client: NodeClient| {
            let req = MigrateRequest {
                desc: Some(desc.clone()),
                action: MigrateAction::Setup as i32,
            };
            async move { client.migrate(req).await }
        };
        let opt = InvokeOpt {
            accurate_epoch: true,
            ignore_transport_error: true,
            ..Default::default()
        };
        self.invoke_with_opt(op, opt).await
    }

    pub async fn commit_migration(&mut self, desc: &MigrationDesc) -> Result<MigrateResponse> {
        let op = |_: InvokeContext, client: NodeClient| {
            let req = MigrateRequest {
                desc: Some(desc.clone()),
                action: MigrateAction::Commit as i32,
            };
            async move { client.migrate(req).await }
        };
        let opt = InvokeOpt {
            ignore_transport_error: true,
            ..Default::default()
        };
        self.invoke_with_opt(op, opt).await
    }

    pub async fn forward(&mut self, req: &ForwardRequest) -> Result<ForwardResponse> {
        let op = |_: InvokeContext, client: NodeClient| {
            let cloned_req = req.clone();
            async move { client.forward(cloned_req).await }
        };
        let opt = InvokeOpt {
            accurate_epoch: true,
            ..Default::default()
        };
        self.invoke_with_opt(op, opt).await
    }

    pub async fn retryable_pull(
        mut self,
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
        let op = |_: InvokeContext, client: NodeClient| {
            let request = PullRequest {
                group_id,
                shard_id,
                last_key: last_key.to_owned(),
            };
            async move { client.pull(request).await }
        };
        let opt = InvokeOpt {
            ignore_transport_error: true,
            ..Default::default()
        };
        self.invoke_with_opt(op, opt).await
    }
}

impl RetryableShardChunkStreaming {
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
                    if let Err(e) = self.client.apply_status(status, &InvokeOpt::default()) {
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

impl futures::Stream for RetryableShardChunkStreaming {
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

#[inline]
fn is_read_only_request(request: &Request) -> bool {
    matches!(request, Request::Get(_) | Request::PrefixList(_))
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

fn move_replica_to_first_element(replicas: &mut Vec<ReplicaDesc>, replica: ReplicaDesc) {
    let idx = if let Some(idx) = replicas.iter().position(|r| r.node_id == replica.node_id) {
        idx
    } else {
        replicas.push(replica);
        replicas.len() - 1
    };
    if idx != 0 {
        replicas.swap(0, idx)
    }
}
