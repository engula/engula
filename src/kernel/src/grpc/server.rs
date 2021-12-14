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

use super::{compose::Kernel as ComposeKernel, proto::*};
use crate::{manifest::Manifest, Kernel, KernelUpdate};

pub struct Server<M: Manifest> {
    kernel: ComposeKernel<M>,
}

impl<M: Manifest> Server<M> {
    pub fn new(kernel: ComposeKernel<M>) -> Self {
        Server { kernel }
    }

    pub fn into_service(self) -> kernel_server::KernelServer<Self> {
        kernel_server::KernelServer::new(self)
    }
}

#[tonic::async_trait]
impl<M: Manifest> kernel_server::Kernel for Server<M> {
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

    async fn place_lookup(
        &self,
        _request: Request<PlaceLookupRequest>,
    ) -> Result<Response<PlaceLookupResponse>, Status> {
        Ok(Response::new(PlaceLookupResponse {
            journal_address: self.kernel.get_journal_addr().to_owned(),
            storage_address: self.kernel.get_storage_addr().to_owned(),
        }))
    }
}
