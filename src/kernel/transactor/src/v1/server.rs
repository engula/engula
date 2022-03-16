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

use engula_apis::v1::*;
use tonic::{Request, Response, Status};

use super::Transactor;

#[derive(Clone)]
pub struct Server {
    transactor: Transactor,
}

impl Default for Server {
    fn default() -> Self {
        let transactor = Transactor::default();
        Self::new(transactor)
    }
}

impl Server {
    pub fn new(transactor: Transactor) -> Self {
        Self { transactor }
    }

    pub fn into_service(self) -> engula_server::EngulaServer<Self> {
        engula_server::EngulaServer::new(self)
    }
}

#[tonic::async_trait]
impl engula_server::Engula for Server {
    async fn batch(&self, req: Request<BatchRequest>) -> Result<Response<BatchResponse>, Status> {
        let req = req.into_inner();
        let res = self.transactor.batch(req).await?;
        Ok(Response::new(res))
    }
}
