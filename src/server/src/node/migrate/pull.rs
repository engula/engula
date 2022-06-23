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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use engula_api::server::v1::*;
use engula_client::{NodeClient, Router};
use futures::StreamExt;
use tracing::warn;

use crate::{node::Replica, serverpb::v1::*, Error, Result};

struct GroupClient {
    group_id: u64,
    shard_id: u64,

    /// Node id to node client.
    node_clients: HashMap<u64, NodeClient>,

    #[allow(unused)]
    epoch: u64,
    leader_node_id: Option<u64>,
    replicas: Vec<ReplicaDesc>,
    next_access_index: usize,

    router: Router,
}

impl GroupClient {
    fn new(group_id: u64, shard_id: u64, router: Router) -> Self {
        GroupClient {
            group_id,
            shard_id,
            router,

            node_clients: HashMap::default(),
            epoch: 0,
            leader_node_id: None,
            replicas: Vec::default(),
            next_access_index: 0,
        }
    }

    async fn pull(&mut self, last_key: &[u8]) -> Result<tonic::Streaming<ShardChunk>> {
        loop {
            let client = self.recommend_client().await;

            let request = PullRequest {
                group_id: self.group_id,
                shard_id: self.shard_id,
                last_key: last_key.to_owned(),
            };
            match client.pull(request).await {
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

        if let Some(address) = self.router.find_node_addr(node_id) {
            match NodeClient::connect(address.clone()).await {
                Ok(client) => {
                    self.node_clients.insert(node_id, client.clone());
                    return Some(client);
                }
                Err(err) => {
                    warn!("connect to node {} address {}: {}", node_id, address, err);
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

pub async fn pull_shard(router: Router, replica: Arc<Replica>, migrate_meta: MigrateMeta) {
    let info = replica.replica_info();
    let shard_id = migrate_meta.shard_desc.as_ref().unwrap().id;
    let mut group_client = GroupClient::new(migrate_meta.src_group_id, shard_id, router.clone());
    while let Ok(()) = replica.on_leader(true).await {
        match pull_shard_round(&mut group_client, replica.as_ref(), &migrate_meta).await {
            Ok(()) => {
                // TODO(walter)
                // update migrate state to half finished.
                return;
            }
            Err(err) => {
                warn!(
                    "replica {} pull shard {}: {}",
                    info.replica_id, shard_id, err
                );
            }
        }
    }
}

async fn pull_shard_round(
    group_client: &mut GroupClient,
    replica: &Replica,
    migrate_meta: &MigrateMeta,
) -> Result<()> {
    let shard_id = migrate_meta.shard_desc.as_ref().unwrap().id;
    let mut shard_chunk_stream = group_client.pull(&migrate_meta.last_migrated_key).await?;
    while let Some(shard_chunk) = shard_chunk_stream.next().await {
        let shard_chunk = shard_chunk?;
        replica.ingest(shard_id, shard_chunk).await?;
    }
    Ok(())
}

pub struct ShardChunkStream {
    shard_id: u64,
    last_key: Vec<u8>,
    replica: Arc<Replica>,
}

impl ShardChunkStream {
    pub fn new(shard_id: u64, last_key: Vec<u8>, replica: Arc<Replica>) -> Self {
        ShardChunkStream {
            shard_id,
            last_key,
            replica,
        }
    }

    async fn next_shard_chunk(&mut self) -> Result<Option<ShardChunk>> {
        let shard_chunk = self
            .replica
            .fetch_shard_chunk(self.shard_id, &self.last_key)
            .await?;
        if shard_chunk.data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(shard_chunk))
        }
    }
}

impl futures::Stream for ShardChunkStream {
    type Item = std::result::Result<ShardChunk, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let future = self.get_mut().next_shard_chunk();
        futures::pin_mut!(future);
        match future.poll(cx) {
            Poll::Ready(Ok(chunk)) => Poll::Ready(chunk.map(Ok)),
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}
