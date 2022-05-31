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
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use engula_api::server::v1::{
    node_server::NodeServer, root_server::RootServer, GroupDesc, JoinNodeRequest, JoinNodeResponse,
    NodeDesc, ReplicaDesc,
};
use tracing::{debug, info, warn};

use crate::{
    node::{state_engine::StateEngine, Node},
    runtime::Executor,
    serverpb::v1::{raft_server::RaftServer, NodeIdent, ReplicaState},
    Error, Result, Server,
};

// TODO(walter) root cluster and first replica id.
const ROOT_GROUP_ID: u64 = 0;
const FIRST_REPLICA_ID: u64 = 0;
const FIRST_NODE_ID: u64 = 0;

/// The main entrance of engula server.
#[allow(unused)]
pub fn run(
    executor: Executor,
    path: PathBuf,
    addr: String,
    init: bool,
    join_list: Vec<String>,
) -> Result<()> {
    let raw_db = Arc::new(open_engine(path)?);
    let state_engine = StateEngine::new(raw_db.clone())?;
    let node = Node::new(raw_db, state_engine, executor.clone());

    executor.block_on(async {
        bootstrap_or_join_cluster(&node, &addr, init, join_list).await?;
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

pub(crate) fn open_engine<P: AsRef<Path>>(path: P) -> Result<rocksdb::DB> {
    use rocksdb::{Options, DB};

    std::fs::create_dir_all(&path)?;

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // List column families and open database with column families.
    match DB::list_cf(&Options::default(), &path) {
        Ok(cfs) => {
            debug!("open local db with {} column families", cfs.len());
            Ok(DB::open_cf(&opts, path, cfs)?)
        }
        Err(e) => {
            if e.as_ref().ends_with("CURRENT: No such file or directory") {
                info!("create new local db");
                Ok(DB::open(&opts, &path)?)
            } else {
                Err(e.into())
            }
        }
    }
}

async fn bootstrap_or_join_cluster(
    node: &Node,
    addr: &str,
    init: bool,
    join_list: Vec<String>,
) -> Result<()> {
    let state_engine = node.state_engine();
    if let Some(node_ident) = state_engine.read_ident().await? {
        info!(
            "both cluster and node are initialized, node id {}",
            node_ident.node_id
        );
        return Ok(());
    }

    if init {
        bootstrap_cluster(node, addr).await?;
    } else {
        try_join_cluster(state_engine, addr, join_list).await?;
    }

    Ok(())
}

#[allow(unused)]
async fn try_join_cluster(
    state_engine: &StateEngine,
    local_addr: &str,
    join_list: Vec<String>,
) -> Result<()> {
    info!("try join a bootstrapted cluster");

    let join_list = join_list
        .iter()
        .filter(|addr| *addr != local_addr)
        .collect::<Vec<_>>();

    if join_list.is_empty() {
        return Err(Error::Invalid("the filtered join list is empty".into()));
    }

    let mut backoff: u64 = 1;
    'OUTER: loop {
        for addr in &join_list {
            match issue_join_request(addr, local_addr).await {
                Ok(resp) => {
                    save_node_ident(state_engine, resp.cluster_id, resp.node_id).await?;
                    break 'OUTER;
                }
                Err(e) => {
                    warn!(err = ?e, root = ?addr, "issue join request to root server");
                }
            }
        }

        std::thread::sleep(Duration::from_secs(backoff));
        backoff = std::cmp::min(backoff, 120);
    }
    Ok(())
}

async fn issue_join_request(target_addr: &str, local_addr: &str) -> Result<JoinNodeResponse> {
    use engula_api::server::v1::root_client::RootClient;
    use tonic::Request;

    let mut client = RootClient::connect(format!("http://{}", target_addr)).await?;
    let resp = client
        .join(Request::new(JoinNodeRequest {
            addr: local_addr.to_owned(),
        }))
        .await
        .unwrap();
    Ok(resp.into_inner())
}

async fn bootstrap_cluster(node: &Node, addr: &str) -> Result<()> {
    info!("'--init' is specified, try bootstrap cluster");

    // TODO(walter) clean staled data in db.
    write_initial_cluster_data(node, addr).await?;

    // The first replica in the cluster has been created. It has only one member
    // and does not need to wait for other replicas, so it can directly enter the
    // normal state.
    let state_engine = node.state_engine();
    state_engine
        .save_replica_state(ROOT_GROUP_ID, FIRST_REPLICA_ID, ReplicaState::Normal)
        .await?;

    // TODO(walter) generate cluster id.
    save_node_ident(state_engine, vec![], FIRST_NODE_ID).await?;

    info!("bootstrap cluster successfully");

    Ok(())
}

async fn save_node_ident(
    state_engine: &StateEngine,
    cluster_id: Vec<u8>,
    node_id: u64,
) -> Result<()> {
    let node_ident = NodeIdent {
        cluster_id,
        node_id,
    };
    state_engine.save_ident(node_ident).await?;

    info!("save node ident, node id {}", node_id);

    Ok(())
}

async fn write_initial_cluster_data(node: &Node, addr: &str) -> Result<()> {
    let state_engine = node.state_engine();

    // TODO(walter) write initial cluster data.
    // - meta shards
    // Create the first raft group of cluster, this node is the only member of the raft group.
    let group = GroupDesc {
        id: ROOT_GROUP_ID,
        shards: vec![],
        replicas: vec![ReplicaDesc {
            id: FIRST_REPLICA_ID,
            node_id: FIRST_NODE_ID,
        }],
    };
    node.create_replica(FIRST_REPLICA_ID, group, false).await?;

    let node_desc = NodeDesc {
        id: FIRST_NODE_ID,
        addr: addr.to_owned(),
    };
    state_engine.save_root_nodes(vec![node_desc]).await?;

    Ok(())
}

async fn recover_groups(node: &Node) -> Result<()> {
    node.recover().await?;
    Ok(())
}
