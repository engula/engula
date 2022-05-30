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

#[allow(unused)]
#[tonic::async_trait]
impl node_server::Node for Server {
    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> Result<Response<BatchResponse>, Status> {
        let batch_request = request.into_inner();
        let (sender, mut receiver) = mpsc::channel(batch_request.requests.len());
        for request in batch_request.requests {
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
                        .unwrap_or_else(error_2_response);
                    task_tx.try_send(response).unwrap_or_default();
                },
            );
        }

        let mut responses = vec![];
        while let Some(response) = receiver.next().await {
            responses.push(response);
        }

        Ok(Response::new(BatchResponse { responses }))
    }

    async fn get_root(
        &self,
        request: Request<GetRootRequest>,
    ) -> Result<Response<GetRootResponse>, Status> {
        todo!()
    }

    async fn create_replica(
        &self,
        request: Request<CreateReplicaRequest>,
    ) -> Result<Response<CreateReplicaResponse>, Status> {
        let request = request.into_inner();
        let group_desc = request
            .group
            .ok_or_else(|| Status::invalid_argument("group is empty"))?;
        let group_id = group_desc.id;
        let replica_id = request.replica_id;
        self.node.create_replica(replica_id, group_desc).await?;
        Ok(Response::new(CreateReplicaResponse {}))
    }
}

impl Server {
    async fn execute_request(&self, request: GroupRequest) -> crate::Result<GroupResponse> {
        let route_table = self.node.replica_table();
        let replica = match route_table.find(request.group_id) {
            Some(replica) => replica,
            None => {
                return Err(Error::StaledRequest(request.group_id));
            }
        };
        replica.execute(&request).await
    }
}

fn error_2_response(err: Error) -> GroupResponse {
    use engula_api::server::v1::Status;

    let status: tonic::Status = err.into();
    GroupResponse {
        response: None,
        status: Some(Status {
            code: status.code() as i32,
            message: status.message().to_owned(),
        }),
    }
}
