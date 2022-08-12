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

use engula_api::server::v1::*;
use futures::{channel::mpsc, StreamExt};
use tonic::{Request, Response, Status};

use super::metrics::*;
use crate::{
    node::migrate::ShardChunkStream, record_latency, record_latency_opt, runtime::TaskPriority,
    Error, Server,
};

#[tonic::async_trait]
impl node_server::Node for Server {
    type PullStream = ShardChunkStream;

    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> Result<Response<BatchResponse>, Status> {
        let batch_request = request.into_inner();
        record_latency!(take_batch_request_metrics(&batch_request));

        let (sender, mut receiver) = mpsc::channel(batch_request.requests.len());
        for (index, request) in batch_request.requests.into_iter().enumerate() {
            let server = self.clone();
            let task_tag = request.group_id.to_le_bytes();
            let mut task_tx = sender.clone();
            self.node.executor().spawn(
                Some(task_tag.as_slice()),
                TaskPriority::Middle,
                async move {
                    record_latency_opt!(take_group_request_metrics(&request));
                    let response = server
                        .node
                        .execute_request(request)
                        .await
                        .unwrap_or_else(error_to_response);
                    task_tx.try_send((index, response)).unwrap_or_default();
                },
            );
        }
        drop(sender);

        let mut responses = vec![];
        while let Some((index, response)) = receiver.next().await {
            responses.push((index, response));
        }
        responses.sort_unstable_by_key(|(index, _)| *index);

        Ok(Response::new(BatchResponse {
            responses: responses.into_iter().map(|(_, resp)| resp).collect(),
        }))
    }

    async fn get_root(
        &self,
        _request: Request<GetRootRequest>,
    ) -> Result<Response<GetRootResponse>, Status> {
        record_latency!(take_get_root_request_metrics());
        let root = self.node.get_root().await;
        Ok(Response::new(GetRootResponse { root: Some(root) }))
    }

    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaResponse>, Status> {
        record_latency!(take_create_replica_request_metrics());
        let request = request.into_inner();
        let group_desc = request
            .group
            .ok_or_else(|| Status::invalid_argument("the field `group` is empty"))?;
        let replica_id = request.replica_id;
        self.node.create_replica(replica_id, group_desc).await?;
        Ok(Response::new(CreateReplicaResponse {}))
    }

    async fn remove_replica(
        &self,
        request: Request<RemoveReplicaRequest>,
    ) -> Result<Response<RemoveReplicaResponse>, Status> {
        record_latency!(take_remove_replica_request_metrics());
        let request = request.into_inner();
        let group_desc = request
            .group
            .ok_or_else(|| Status::invalid_argument("the field `group` is empty"))?;
        let replica_id = request.replica_id;
        self.node.remove_replica(replica_id, &group_desc).await?;
        Ok(Response::new(RemoveReplicaResponse {}))
    }

    async fn root_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        use engula_api::server::v1::{piggyback_request, piggyback_response};

        record_latency!(take_root_heartbeat_request_metrics());
        let request = request.into_inner();
        let mut piggybacks_resps = Vec::with_capacity(request.piggybacks.len());

        for req in request.piggybacks {
            let info = match req.info.unwrap() {
                piggyback_request::Info::SyncRoot(req) => {
                    piggyback_response::Info::SyncRoot(self.update_root(req).await?)
                }
                piggyback_request::Info::CollectStats(req) => {
                    piggyback_response::Info::CollectStats(self.node.collect_stats(&req).await)
                }
                piggyback_request::Info::CollectGroupDetail(req) => {
                    piggyback_response::Info::CollectGroupDetail(
                        self.node.collect_group_detail(&req).await,
                    )
                }
                piggyback_request::Info::CollectMigrationState(req) => {
                    piggyback_response::Info::CollectMigrationState(
                        self.node.collect_migration_state(&req).await,
                    )
                }
                piggyback_request::Info::CollectScheduleState(req) => {
                    piggyback_response::Info::CollectScheduleState(
                        self.node.collect_schedule_state(&req).await,
                    )
                }
            };
            piggybacks_resps.push(PiggybackResponse { info: Some(info) });
        }

        let root = self.node.get_root().await;
        Ok(Response::new(HeartbeatResponse {
            timestamp: request.timestamp,
            root_epoch: root.epoch,
            piggybacks: piggybacks_resps,
        }))
    }

    async fn migrate(
        &self,
        request: Request<MigrateRequest>,
    ) -> Result<Response<MigrateResponse>, Status> {
        record_latency!(take_migrate_request_metrics());
        let req = request.into_inner();
        let resp = self.node.migrate(req).await?;
        Ok(Response::new(resp))
    }

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        record_latency!(take_pull_request_metrics());
        let request = request.into_inner();
        let stream = self.node.pull_shard_chunks(request).await?;
        Ok(Response::new(stream))
    }

    async fn forward(
        &self,
        request: Request<ForwardRequest>,
    ) -> Result<Response<ForwardResponse>, Status> {
        record_latency!(take_forward_request_metrics());
        let req = request.into_inner();
        let resp = self.node.forward(req).await?;
        Ok(Response::new(resp))
    }
}

impl Server {
    async fn update_root(&self, req: SyncRootRequest) -> crate::Result<SyncRootResponse> {
        if let Some(root) = req.root {
            self.node.update_root(root).await?;
        }
        Ok(SyncRootResponse {})
    }
}

fn error_to_response(err: Error) -> GroupResponse {
    GroupResponse {
        response: None,
        error: Some(err.into()),
    }
}
