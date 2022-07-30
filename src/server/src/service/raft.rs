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

use futures::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, warn};

use crate::{
    raftgroup::snap::send::{send_snapshot, SnapshotChunkStream},
    serverpb::v1::*,
    service::metrics::*,
    Server,
};

#[tonic::async_trait]
impl raft_server::Raft for Server {
    type RetriveSnapshotStream = SnapshotChunkStream;

    async fn send_message(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<RaftDone>, Status> {
        let mut in_stream = request.into_inner();
        while let Some(next_msg) = in_stream.next().await {
            match next_msg {
                Ok(msg) => {
                    RAFT_SERVICE_MSG_REQUEST_TOTAL.inc();
                    RAFT_SERVICE_MSG_BATCH_SIZE.observe(msg.messages.len() as f64);

                    let target_replica_id = match msg.to_replica.as_ref() {
                        None => {
                            error!("receive messages {:?} without to replica", msg);
                            break;
                        }
                        Some(r) => r.id,
                    };
                    let replica = msg.from_replica.as_ref().unwrap();
                    let from_replica_id = replica.id;
                    let from_node_id = replica.node_id;
                    if let Some(mut sender) = self.node.raft_route_table().find(target_replica_id) {
                        if sender.step(msg).is_ok() {
                            continue;
                        }
                    }
                    warn!(
                        "receive message from node {from_node_id} replica {from_replica_id} to a not existed replica {target_replica_id}",
                    );
                    break;
                }
                Err(e) => {
                    warn!(err = ?e, "receive messages");
                    break;
                }
            }
        }
        Ok(Response::new(RaftDone {}))
    }

    async fn retrive_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotChunkStream>, Status> {
        RAFT_SERVICE_SNAPSHOT_REQUEST_TOTAL.inc();

        let request = request.into_inner();
        let snap_mgr = self.node.raft_manager().snapshot_manager();

        let stream = send_snapshot(snap_mgr, request.replica_id, request.snapshot_id).await?;
        Ok(Response::new(stream))
    }
}
