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

use std::{net::SocketAddr, sync::Arc};

use axum::Server;

use crate::{error::Result, node::Node, node_router::route};

/// An HTTP server that serves a node.
pub struct NodeServer {
    node: Arc<Node>,
}

impl NodeServer {
    pub fn new(node: Node) -> NodeServer {
        NodeServer {
            node: Arc::new(node),
        }
    }

    pub async fn bind(&self, addr: SocketAddr) -> Result<()> {
        let router = route(self.node.clone());
        Server::bind(&addr)
            .serve(router.into_make_service())
            .await?;
        Ok(())
    }
}
