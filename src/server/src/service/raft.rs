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
use tracing::warn;

use crate::{
    node::replica::raft::snap::send::{send_snapshot, SnapshotChunkStream},
    serverpb::v1::*,
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
        while let Some(result) = in_stream.next().await {
            match result {
                Ok(msg) => {
                    self.handle_raft_message(msg).await;
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
        let request = request.into_inner();
        let snap_mgr = self.node.raft_manager().snapshot_manager();

        let stream = send_snapshot(snap_mgr, request.replica_id, request.snapshot_id).await?;
        Ok(Response::new(stream))
    }
}

impl Server {
    async fn handle_raft_message(&self, msg: RaftMessage) {
        let target_replica = msg.to_replica.as_ref().expect("to_replica is required");
        if let Some(mut sender) = self.node.raft_route_table().find(target_replica.id) {
            sender.step(msg).expect("raft are shutdown?");
        } else {
            todo!("target replica not found");
        }
    }
}
