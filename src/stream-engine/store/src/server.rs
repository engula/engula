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

use futures::Stream;
use stream_engine_proto::*;
use tonic::{async_trait, Request, Response, Status};

use crate::Result;

type TonicResult<T> = std::result::Result<T, Status>;

#[derive(Debug)]
pub struct ReplicaReader {}

#[allow(unused)]
impl Stream for ReplicaReader {
    type Item = TonicResult<ReadResponse>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

#[derive(Debug)]
pub struct Server {}

#[allow(unused, dead_code)]
impl Server {
    pub fn new() -> Self {
        Server {}
    }

    pub fn into_service(self) -> store_server::StoreServer<Server> {
        store_server::StoreServer::new(self)
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
#[async_trait]
impl store_server::Store for Server {
    type ReadStream = ReplicaReader;

    async fn mutate(&self, input: Request<MutateRequest>) -> TonicResult<Response<MutateResponse>> {
        todo!();
    }

    async fn read(&self, input: Request<ReadRequest>) -> TonicResult<Response<Self::ReadStream>> {
        todo!();
    }
}

#[allow(unused, dead_code)]
impl Server {
    async fn handle_write(&self, write: WriteRequest) -> Result<WriteResponse> {
        todo!()
    }

    async fn handle_seal(&self, seal: SealRequest) -> Result<SealResponse> {
        todo!()
    }
}
