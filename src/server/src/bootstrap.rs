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

use engula_api::server::v1::{node_server::NodeServer, root_server::RootServer};

use crate::{engula::server::v1::raft_server::RaftServer, runtime::Executor, Result, Server};

/// Listen and serve incoming rpc requests.
async fn bootstrap_services(addr: &str, server: Server) -> Result<()> {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    let listener = TcpListener::bind(addr).await?;
    let listener = TcpListenerStream::new(listener);

    Server::builder()
        .add_service(NodeServer::new(server.clone()))
        .add_service(RaftServer::new(server.clone()))
        .add_service(RootServer::new(server.clone()))
        .serve_with_incoming(listener)
        .await?;

    Ok(())
}

/// The main entrance of engula server.
#[allow(unused)]
pub fn run(executor: Executor, addr: String, init: bool, join: Option<String>) -> Result<()> {
    // TODO(walter) setup node and recover groups.
    let server = Server {};
    let handle = executor.spawn(None, crate::runtime::TaskPriority::High, async move {
        bootstrap_services(&addr, server).await
    });

    if init {
        // TODO(walter) might initial cluster
    }

    executor.block_on(handle)
}
