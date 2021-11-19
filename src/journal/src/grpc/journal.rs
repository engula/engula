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

use std::marker::PhantomData;

use super::{
    client::Client,
    error::Result,
    proto::{CreateStreamRequest, DeleteStreamRequest},
    stream::RemoteStream,
};
use crate::{async_trait, Journal, Timestamp};

pub struct RemoteJournal<T: Timestamp> {
    client: Client,
    _t: PhantomData<T>,
}

impl<T: Timestamp> RemoteJournal<T> {
    pub async fn connect(addr: &str) -> Result<RemoteJournal<T>> {
        let client = Client::connect(addr).await?;
        Ok(RemoteJournal {
            client,
            _t: PhantomData,
        })
    }
}

#[async_trait]
impl<T: Timestamp> Journal<RemoteStream<T>> for RemoteJournal<T> {
    async fn stream(&self, name: &str) -> Result<RemoteStream<T>> {
        Ok(RemoteStream::new(self.client.clone(), name.to_owned()))
    }

    async fn create_stream(&self, name: &str) -> Result<RemoteStream<T>> {
        let input = CreateStreamRequest {
            stream: name.to_owned(),
        };
        self.client.create_stream(input).await?;
        self.stream(name).await
    }

    async fn delete_stream(&self, name: &str) -> Result<()> {
        let input = DeleteStreamRequest {
            stream: name.to_owned(),
        };
        self.client.delete_stream(input).await?;
        Ok(())
    }
}
