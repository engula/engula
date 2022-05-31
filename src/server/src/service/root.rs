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

use engula_api::{server::v1::*, v1::*};
use tonic::{Request, Response, Status};

use crate::Server;

pub struct WatchStream;

#[allow(unused)]
#[tonic::async_trait]
impl root_server::Root for Server {
    type WatchStream = WatchStream;

    async fn admin(
        &self,
        request: Request<AdminRequest>,
    ) -> Result<Response<AdminResponse>, Status> {
        todo!()
    }

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        todo!()
    }

    async fn join(
        &self,
        request: Request<JoinNodeRequest>,
    ) -> Result<Response<JoinNodeResponse>, Status> {
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(2);

        Ok(Response::new(JoinNodeResponse {
            cluster_id: vec![],
            node_id: COUNTER.fetch_add(1, Ordering::Relaxed),
        }))
    }
}

#[allow(unused)]
impl futures::Stream for WatchStream {
    type Item = Result<WatchResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
