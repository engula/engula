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

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::Stream;
use tonic::{Request, Response, Status};

use super::proto::server as serverpb;

struct ReadStream {}

impl Stream for ReadStream {
    type Item = Result<serverpb::Entry, Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

struct Server {}

#[async_trait]
#[allow(unused)]
impl serverpb::shared_journal_server::SharedJournal for Server {
    type ReadStream = ReadStream;

    async fn store(
        &self,
        input: Request<serverpb::StoreRequest>,
    ) -> Result<Response<serverpb::StoreResponse>, Status> {
        todo!()
    }

    async fn read(
        &self,
        input: Request<serverpb::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        todo!()
    }
}
