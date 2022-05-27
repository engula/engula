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
use tonic::{Request, Response, Status};

use crate::Server;

#[allow(unused)]
#[tonic::async_trait]
impl node_server::Node for Server {
    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> Result<Response<BatchResponse>, Status> {
        todo!()
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
        todo!()
    }
}
