// Copyright 2021 The Engula Authors.
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

use futures::StreamExt;
use tonic::{Request, Response, Status};

use super::proto::*;
use crate::{Kernel, KernelUpdate};

pub struct Server<K: Kernel> {
    kernel: K,
}

impl<K: Kernel> Server<K> {
    pub fn new(kernel: K) -> Self {
        Server { kernel }
    }

    pub fn into_service(self) -> kernel_server::KernelServer<Server<K>> {
        kernel_server::KernelServer::new(self)
    }
}

#[tonic::async_trait]
impl<K: Kernel> kernel_server::Kernel for Server<K> {
    type VersionUpdatesStream =
        Box<dyn futures::Stream<Item = Result<VersionUpdatesResponse, Status>> + Send + Unpin>;

    async fn apply_update(
        &self,
        request: Request<ApplyUpdateRequest>,
    ) -> Result<Response<ApplyUpdateResponse>, Status> {
        let input = request.into_inner();
        if let Some(update) = input.version_update {
            self.kernel.apply_update(KernelUpdate { update }).await?;
        }
        Ok(Response::new(ApplyUpdateResponse {}))
    }

    async fn current_version(
        &self,
        _request: Request<CurrentVersionRequest>,
    ) -> Result<Response<CurrentVersionResponse>, Status> {
        let version = self.kernel.current_version().await?;
        Ok(Response::new(CurrentVersionResponse {
            version: Some((*version).clone()),
        }))
    }

    async fn version_updates(
        &self,
        request: Request<VersionUpdatesRequest>,
    ) -> Result<Response<Self::VersionUpdatesStream>, Status> {
        let input = request.into_inner();
        let updates_stream = self.kernel.version_updates(input.sequence).await;
        Ok(Response::new(Box::new(updates_stream.map(
            |result| match result {
                Ok(version_update) => Ok(VersionUpdatesResponse {
                    version_update: Some((*version_update).clone()),
                }),
                Err(e) => Err(e.into()),
            },
        ))))
    }
}
