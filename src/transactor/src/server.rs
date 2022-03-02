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

use engula_apis::*;
use engula_cooperator::Cooperator;
use engula_supervisor::Supervisor;
use tonic::{Request, Response};

pub struct Server {
    supervisor: Supervisor,
    cooperator: Cooperator,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        let supervisor = Supervisor::new();
        let cooperator = Cooperator::new(supervisor.clone());
        Self {
            supervisor,
            cooperator,
        }
    }

    pub fn into_service(self) -> engula_server::EngulaServer<Self> {
        engula_server::EngulaServer::new(self)
    }
}

type TonicResult<T> = std::result::Result<T, tonic::Status>;

#[tonic::async_trait]
impl engula_server::Engula for Server {
    async fn txn(&self, req: Request<TxnRequest>) -> TonicResult<Response<TxnResponse>> {
        let req = req.into_inner();
        let res = self.cooperator.txn(req).await?;
        Ok(Response::new(res))
    }

    async fn database(
        &self,
        req: Request<DatabaseRequest>,
    ) -> TonicResult<Response<DatabaseResponse>> {
        let req = req.into_inner();
        let res = self.supervisor.database(req).await?;
        Ok(Response::new(res))
    }

    async fn collection(
        &self,
        req: Request<CollectionRequest>,
    ) -> TonicResult<Response<CollectionResponse>> {
        let req = req.into_inner();
        let res = self.supervisor.collection(req).await?;
        Ok(Response::new(res))
    }
}
