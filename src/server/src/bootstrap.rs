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

use std::sync::Arc;

use engula_api::server::v1::{node_server::NodeServer, root_server::RootServer, GroupDesc};

use crate::{
    node::{state_engine::StateEngine, Node},
    runtime::Executor,
    serverpb::v1::{raft_server::RaftServer, NodeIdent, ReplicaState},
    Result, Server,
};

/// The main entrance of engula server.
#[allow(unused)]
pub fn run(executor: Executor, addr: String, init: bool, join_list: Vec<String>) -> Result<()> {
    let raw_db = Arc::new(open_engine()?);
    let state_engine = StateEngine::new(raw_db.clone())?;
    let node = Node::new(raw_db, state_engine);

    executor.block_on(async {
        bootstrap_or_join_cluster(&node, init, join_list).await?;
        recover_groups(&node).await
    })?;

    let server = Server {
        node: Arc::new(node),
    };
    let handle = executor.spawn(None, crate::runtime::TaskPriority::High, async move {
        bootstrap_services(&addr, server).await
    });

    executor.block_on(handle)
}

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

fn open_engine() -> Result<rocksdb::DB> {
    use rocksdb::{Options, DB};

    // FIXME(walter) config engine path.
    let path = "/tmp/engula-server/";
    std::fs::create_dir_all(&path)?;

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    Ok(DB::open(&opts, path)?)
}

async fn bootstrap_or_join_cluster(node: &Node, init: bool, join_list: Vec<String>) -> Result<()> {
    let state_engine = node.state_engine();
    if state_engine.read_ident().await?.is_some() {
        return Ok(());
    }

    if init {
        bootstrap_cluster(node).await?;
    } else {
        try_join_cluster(state_engine, join_list).await?;
    }

    Ok(())
}

#[allow(unused)]
async fn try_join_cluster(state_engine: &StateEngine, join_list: Vec<String>) -> Result<()> {
    // TODO(walter) filter self
    todo!()
}

async fn bootstrap_cluster(node: &Node) -> Result<()> {
    // TODO(walter) clean staled data in db.
    write_initial_cluster_data(node).await?;

    // TODO(walter) root cluster and first replica id.
    const ROOT_GROUP_ID: u64 = 0;
    const FIRST_REPLICA_ID: u64 = 0;
    let state_engine = node.state_engine();
    state_engine
        .save_replica_state(ROOT_GROUP_ID, FIRST_REPLICA_ID, ReplicaState::Normal)
        .await?;

    // TODO(walter) first node id and generate cluster id.
    const FIRST_NODE_ID: u64 = 0;
    let node_ident = NodeIdent {
        cluster_id: vec![],
        node_id: FIRST_NODE_ID,
    };
    state_engine.save_ident(node_ident).await?;

    // TODO(walter) log cluster is bootstrapted.

    Ok(())
}

async fn write_initial_cluster_data(node: &Node) -> Result<()> {
    let state_engine = node.state_engine();

    // TODO(walter) root cluster and first replica id.
    const ROOT_GROUP_ID: u64 = 0;
    const FIRST_REPLICA_ID: u64 = 0;

    // TODO(walter) write initial cluster data.
    // - meta shards
    let group = GroupDesc {
        id: ROOT_GROUP_ID,
        shards: vec![],
        replicas: vec![],
    };
    node.create_replica(FIRST_REPLICA_ID, group).await?;

    // FIXME(walter) fill root nodes.
    state_engine.save_root_nodes(vec![]).await?;

    Ok(())
}

#[allow(unused)]
async fn recover_groups(node: &Node) -> Result<()> {
    // TODO(walter) recover groups
    node.recover().await?;
    Ok(())
}
