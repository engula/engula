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
use std::sync::Arc;

use engula_api::server::v1::{NodeDesc, ReplicaDesc};
use futures::{channel::mpsc, StreamExt};
use tracing::{debug, warn};

use super::RaftNodeFacade;
use crate::{
    node::route_table::RaftRouteTable,
    runtime::{Executor, TaskPriority},
    serverpb::v1::{raft_client::RaftClient, RaftMessage, SnapshotChunk, SnapshotRequest},
    Result,
};

struct StreamingRequest {
    from: ReplicaDesc,
    to: ReplicaDesc,

    receiver: mpsc::UnboundedReceiver<RaftMessage>,
}

struct StreamingTask {
    resolver: Arc<dyn AddressResolver>,
    raft_node: RaftNodeFacade,
    request: StreamingRequest,
}

/// An abstraction for resolving address by node id.
#[crate::async_trait]
pub trait AddressResolver: Send + Sync {
    async fn resolve(&self, node_id: u64) -> Result<NodeDesc>;
}

/// A logic connection between two nodes. A [`Channel`] is bind to a specific target,
/// the name lookup are finished by internal machenism.
#[derive(Clone)]
pub struct Channel {
    transport_mgr: TransportManager,
    sender: Option<mpsc::UnboundedSender<RaftMessage>>,
}

/// Manage transports. This structure is used by all groups.
///
/// A transport is recycled by manager, if it exceeds the idle intervals.
#[derive(Clone)]
pub struct TransportManager
where
    Self: Send + Sync,
{
    executor: Executor,
    resolver: Arc<dyn AddressResolver>,
    sender: mpsc::UnboundedSender<StreamingRequest>,
    route_table: RaftRouteTable,
}

impl Channel {
    pub fn new(mgr: TransportManager) -> Self {
        Channel {
            transport_mgr: mgr,
            sender: None,
        }
    }

    pub fn send_message(&mut self, mut msg: RaftMessage) {
        loop {
            if let Some(sender) = &mut self.sender {
                match sender.unbounded_send(msg) {
                    Ok(()) => return,
                    Err(err) => {
                        msg = err.into_inner();
                    }
                }
            }

            // Try create new connection if we reaches here.
            let (sender, receiver) = mpsc::unbounded();
            let req = StreamingRequest {
                from: msg.from_replica.as_ref().cloned().unwrap(),
                to: msg.to_replica.as_ref().cloned().unwrap(),
                receiver,
            };

            self.transport_mgr.issue_streaming_request(req);
            self.sender = Some(sender);
        }
    }
}

impl TransportManager {
    pub fn build(
        executor: Executor,
        resolver: Arc<dyn AddressResolver>,
        route_table: RaftRouteTable,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded();
        let mgr = TransportManager {
            executor,
            resolver,
            sender,
            route_table,
        };

        let cloned_mgr = mgr.clone();
        mgr.executor.spawn(None, TaskPriority::Low, async move {
            cloned_mgr.run(receiver).await;
        });
        mgr
    }

    #[inline]
    fn issue_streaming_request(&self, stream_request: StreamingRequest) {
        self.sender
            .unbounded_send(stream_request)
            .expect("transport worker lifetime should large that replicas");
    }

    async fn run(self, mut receiver: mpsc::UnboundedReceiver<StreamingRequest>) {
        while let Some(request) = receiver.next().await {
            let raft_node = match self.route_table.find(request.from.id) {
                Some(raft_node) => raft_node,
                None => {
                    debug!(
                        "receive streaming request but no such raft node exists, replica id {}",
                        request.from.id
                    );
                    continue;
                }
            };

            let task = StreamingTask {
                resolver: self.resolver.clone(),
                raft_node,
                request,
            };
            self.executor.spawn(None, TaskPriority::IoHigh, async move {
                task.run().await;
            });
        }
    }
}

impl StreamingTask {
    async fn run(self) {
        let mut raft_node = self.raft_node.clone();
        let target_id = self.request.to.id;
        let from_id = self.request.from.id;
        let node_id = self.request.to.node_id;
        if let Err(e) = self.serve_streaming_request().await {
            warn!("serve request to node {node_id} replica {target_id} from {from_id}: {e:?}");
            raft_node.report_unreachable(target_id);
        }
    }

    async fn serve_streaming_request(self) -> Result<()> {
        let node_desc = resolve_address(&*self.resolver, self.request.to.node_id).await?;
        let address = format!("http://{}", node_desc.addr);
        let mut client = RaftClient::connect(address).await?;
        client.send_message(self.request.receiver).await?;
        Ok(())
    }
}

pub async fn retrive_snapshot(
    trans_mgr: &TransportManager,
    target_replica: ReplicaDesc,
    snapshot_id: Vec<u8>,
) -> Result<impl futures::Stream<Item = std::result::Result<SnapshotChunk, tonic::Status>>> {
    let node_desc = resolve_address(&*trans_mgr.resolver, target_replica.node_id).await?;
    let address = format!("http://{}", node_desc.addr);
    let mut client = RaftClient::connect(address).await?;
    let request = SnapshotRequest {
        replica_id: target_replica.id,
        snapshot_id,
    };
    let resp = client.retrieve_snapshot(request).await?;
    Ok(resp.into_inner())
}

async fn resolve_address(resolver: &dyn AddressResolver, node_id: u64) -> Result<NodeDesc> {
    let mut count = 0;
    loop {
        match resolver.resolve(node_id).await {
            Ok(node_desc) => return Ok(node_desc),
            Err(err) => {
                debug!(err = ?err, "resolve address of node {}", node_id);
                count += 1;
                if count == 3 {
                    return Err(err);
                }
            }
        }
    }
}
