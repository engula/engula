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

use tonic::{Request, Response, Status};

use super::{apis::v1::*, Cooperator};

#[derive(Clone)]
pub struct Server {
    cooperator: Cooperator,
}

impl Server {
    pub fn new(cooperator: Cooperator) -> Self {
        Self { cooperator }
    }

    pub fn into_service(self) -> cooperator_server::CooperatorServer<Self> {
        cooperator_server::CooperatorServer::new(self)
    }
}

#[tonic::async_trait]
impl cooperator_server::Cooperator for Server {
    async fn batch(&self, req: Request<BatchRequest>) -> Result<Response<BatchResponse>, Status> {
        let req = req.into_inner();
        let res = self.cooperator.batch(req).await?;
        Ok(Response::new(res))
    }
}
