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
use futures::{channel::mpsc, Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::{runtime::TaskPriority, Error, Server};

pub struct PullStream;

#[tonic::async_trait]
impl node_server::Node for Server {
    type PullStream = PullStream;

    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> Result<Response<BatchResponse>, Status> {
        let batch_request = request.into_inner();
        let (sender, mut receiver) = mpsc::channel(batch_request.requests.len());
        for (index, request) in batch_request.requests.into_iter().enumerate() {
            let server = self.clone();
            let task_tag = request.group_id.to_le_bytes();
            let mut task_tx = sender.clone();
            self.node.executor().spawn(
                Some(task_tag.as_slice()),
                TaskPriority::Middle,
                async move {
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

    #[allow(unused)]
    async fn get_root(
        &self,
        request: Request<GetRootRequest>,
    ) -> Result<Response<GetRootResponse>, Status> {
        let addrs = self.node.get_root().await;
        Ok(Response::new(GetRootResponse { addrs }))
    }

    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaResponse>, Status> {
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
            };
            piggybacks_resps.push(PiggybackResponse { info: Some(info) });
        }

        Ok(Response::new(HeartbeatResponse {
            timestamp: request.timestamp,
            piggybacks: piggybacks_resps,
        }))
    }

    #[allow(unused)]
    async fn migrate_prepare(
        &self,
        request: Request<MigratePrepareRequest>,
    ) -> Result<Response<MigrateResponse>, Status> {
        todo!()
    }

    #[allow(unused)]
    async fn migrate_commit(
        &self,
        request: Request<MigrateCommitRequest>,
    ) -> Result<Response<MigrateResponse>, Status> {
        todo!()
    }

    #[allow(unused)]
    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        todo!()
    }

    #[allow(unused)]
    async fn forward(
        &self,
        request: Request<ForwardRequest>,
    ) -> Result<Response<ForwardResponse>, Status> {
        todo!()
    }
}

impl Server {
    async fn update_root(&self, req: SyncRootRequest) -> crate::Result<SyncRootResponse> {
        self.node.update_root(req.roots).await?;
        Ok(SyncRootResponse {})
    }
}

#[allow(unused)]
impl Stream for PullStream {
    type Item = Result<ShardChunk, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

fn error_to_response(err: Error) -> GroupResponse {
    GroupResponse {
        response: None,
        error: Some(err.into()),
    }
}
