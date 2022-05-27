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

use std::{pin::Pin, task::{Context, Poll}};

use tonic::{Request, Response, Status, Streaming};

use crate::{engula::server::v1::*, Server};

pub struct SnapshotStream;

#[allow(unused)]
#[tonic::async_trait]
impl raft_server::Raft for Server {
    type RetriveSnapshotStream = SnapshotStream;

    async fn send_message(
        &self,
        request: Request<Streaming<RaftMessage>>,
    ) -> Result<Response<RaftDone>, Status> {
        todo!()
    }

    async fn retrive_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::RetriveSnapshotStream>, Status> {
        todo!()
    }
}

#[allow(unused)]
impl futures::Stream for SnapshotStream {
    type Item = Result<SnapshotResponse, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
