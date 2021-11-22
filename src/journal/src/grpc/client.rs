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

use super::{error::Result, proto::*};

type JournalClient = journal_client::JournalClient<Channel>;

#[derive(Clone)]
pub struct Client {
    client: JournalClient,
}

impl Client {
    pub async fn create_stream(&self, input: CreateStreamRequest) -> Result<CreateStreamResponse> {
        let mut client = self.client.clone();
        let response = client.create_stream(input).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_stream(&self, input: DeleteStreamRequest) -> Result<DeleteStreamResponse> {
        let mut client = self.client.clone();
        let response = client.delete_stream(input).await?;
        Ok(response.into_inner())
    }

    pub async fn append_event(&self, input: AppendEventRequest) -> Result<AppendEventResponse> {
        let mut client = self.client.clone();
        let response = client.append_event(input).await?;
        Ok(response.into_inner())
    }

    pub async fn release_events(
        &self,
        input: ReleaseEventsRequest,
    ) -> Result<ReleaseEventsResponse> {
        let mut client = self.client.clone();
        let response = client.release_events(input).await?;
        Ok(response.into_inner())
    }

    pub async fn read_event(
        &self,
        input: ReadEventRequest,
    ) -> Result<Streaming<ReadEventResponse>> {
        let mut client = self.client.clone();
        let response = client.read_event(input).await?;
        Ok(response.into_inner())
    }

    pub async fn connect(addr: &str) -> Result<Client> {
        let client = JournalClient::connect(addr.to_owned()).await?;
        Ok(Client { client })
    }
}
