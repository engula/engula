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
use tonic::{Request, Response, Status};

pub struct Server<S> {
    supervisor: S,
    cooperator: Cooperator,
}

impl<S> Server<S>
where
    S: Supervisor,
{
    pub fn new(supervisor: S) -> Self {
        Self {
            supervisor,
            cooperator: Cooperator::new(),
        }
    }

    pub fn into_service(self) -> engula_server::EngulaServer<Self> {
        engula_server::EngulaServer::new(self)
    }
}

type TonicResult<T> = std::result::Result<T, Status>;

#[tonic::async_trait]
impl<S> engula_server::Engula for Server<S>
where
    S: Supervisor,
{
    async fn txn(&self, req: Request<TxnRequest>) -> TonicResult<Response<TxnResponse>> {
        let req = req.into_inner();
        let res = self.cooperator.execute(req).await?;
        Ok(Response::new(res))
    }

    async fn database(
        &self,
        req: Request<DatabaseRequest>,
    ) -> TonicResult<Response<DatabaseResponse>> {
        self.supervisor.database(req).await
    }

    async fn collection(
        &self,
        req: Request<CollectionRequest>,
    ) -> TonicResult<Response<CollectionResponse>> {
        self.supervisor.collection(req).await
    }
}
