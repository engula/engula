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

use crate::{proto::*, Master};

#[derive(Clone)]
pub struct Server {
    master: Master,
}

impl Server {
    pub fn new(master: Master) -> Self {
        Self { master }
    }

    pub fn into_service(self) -> master_server::MasterServer<Self> {
        master_server::MasterServer::new(self)
    }
}

type TonicResult<T> = std::result::Result<T, Status>;

#[tonic::async_trait]
impl master_server::Master for Server {
    async fn tenant(&self, req: Request<TenantRequest>) -> TonicResult<Response<TenantResponse>> {
        let req = req.into_inner();
        let res = self.master.handle_tenant(req).await?;
        Ok(Response::new(res))
    }

    async fn bucket(&self, req: Request<BucketRequest>) -> TonicResult<Response<BucketResponse>> {
        let req = req.into_inner();
        let res = self.master.handle_bucket(req).await?;
        Ok(Response::new(res))
    }

    async fn ingest(&self, _: Request<IngestRequest>) -> TonicResult<Response<IngestResponse>> {
        todo!()
    }
}
