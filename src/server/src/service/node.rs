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

use crate::{runtime::TaskPriority, Error, Server};

#[tonic::async_trait]
impl node_server::Node for Server {
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
        self.node
            .create_replica(replica_id, group_desc, true)
            .await?;
        self.node.start_replica(replica_id).await?;
        Ok(Response::new(CreateReplicaResponse {}))
    }

    async fn root_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let mut piggybacks_resps = Vec::new();

        for req in request.piggybacks {
            match req.info.unwrap() {
                piggyback_request::Info::SyncRoot(req) => {
                    piggybacks_resps.push(self.update_root(req).await?);
                }
                piggyback_request::Info::CollectStats(_req) => {
                    todo!()
                }
                piggyback_request::Info::CollectGroupDetail(_req) => {
                    todo!()
                }
            }
        }

        Ok(Response::new(HeartbeatResponse {
            timestamp: request.timestamp,
            piggybacks: piggybacks_resps,
        }))
    }
}

impl Server {
    async fn execute_request(&self, request: GroupRequest) -> crate::Result<GroupResponse> {
        let route_table = self.node.replica_table();
        let replica = match route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::GroupNotFound(request.group_id));
            }
        };
        replica.execute(&request).await
    }

    async fn update_root(&self, req: SyncRootRequest) -> crate::Result<PiggybackResponse> {
        self.node.update_root(req.roots).await?;
        Ok(PiggybackResponse {
            info: Some(piggyback_response::Info::SyncRoot(SyncRootResponse {})),
        })
    }
}

fn error_to_response(err: Error) -> GroupResponse {
    use engula_api::server::v1;
    use tonic::Code;

    let net_err = match err {
        Error::InvalidArgument(msg) => v1::Error::status(Code::InvalidArgument.into(), msg),
        Error::GroupNotFound(group_id) => v1::Error::group_not_found(group_id),
        Error::NotLeader(group_id, leader) => v1::Error::not_leader(group_id, leader),
        Error::NotRootLeader(roots) => v1::Error::not_root_leader(roots),
        Error::Transport(inner) => v1::Error::status(Code::Internal.into(), inner.to_string()),
        Error::RocksDb(inner) => v1::Error::status(Code::Internal.into(), inner.to_string()),
        Error::Raft(inner) => v1::Error::status(Code::Internal.into(), inner.to_string()),
        Error::RaftEngine(inner) => v1::Error::status(Code::Internal.into(), inner.to_string()),
        Error::Io(inner) => {
            let status: Status = inner.into();
            v1::Error::status(status.code().into(), status.message())
        }
        err @ Error::DatabaseNotFound(_) => {
            v1::Error::status(Code::Internal.into(), err.to_string())
        }
        err @ Error::InvalidData(_) => v1::Error::status(Code::Internal.into(), err.to_string()),
        err @ Error::ClusterNotMatch => v1::Error::status(Code::Internal.into(), err.to_string()),
        err @ Error::Canceled => v1::Error::status(Code::Cancelled.into(), err.to_string()),
        err @ Error::Rpc(_) => v1::Error::status(Code::Internal.into(), err.to_string()),
        Error::DeadlineExceeded(msg) => v1::Error::status(Code::DeadlineExceeded.into(), msg),
    };

    GroupResponse {
        response: None,
        error: Some(net_err),
    }
}
