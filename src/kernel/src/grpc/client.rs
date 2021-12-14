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

use tonic::{transport::Channel, Streaming};

use super::proto::*;
use crate::Result;

type KernelClient = kernel_client::KernelClient<Channel>;

#[derive(Clone)]
pub struct Client {
    client: KernelClient,
}

impl Client {
    pub async fn apply_update(&self, input: ApplyUpdateRequest) -> Result<ApplyUpdateResponse> {
        let mut client = self.client.clone();
        let resp = client.apply_update(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn current_version(
        &self,
        input: CurrentVersionRequest,
    ) -> Result<CurrentVersionResponse> {
        let mut client = self.client.clone();
        let resp = client.current_version(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn version_updates(
        &self,
        input: VersionUpdatesRequest,
    ) -> Result<Streaming<VersionUpdatesResponse>> {
        let mut client = self.client.clone();
        let resp = client.version_updates(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn place_lookup(&self, input: PlaceLookupRequest) -> Result<PlaceLookupResponse> {
        let mut client = self.client.clone();
        let resp = client.place_lookup(input).await?;
        Ok(resp.into_inner())
    }

    pub async fn connect(addr: &str) -> Result<Client> {
        let client = KernelClient::connect(addr.to_owned()).await?;
        Ok(Client { client })
    }
}
