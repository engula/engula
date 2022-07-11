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
    vec,
};

use engula_api::server::v1::{node_server::NodeServer, root_server::RootServer, *};
use engula_client::{RootClient, Router};
use tracing::{debug, info, warn};

use crate::{
    node::{engine::StateEngine, resolver::AddressResolver, Node},
    root::{Root, Schema},
    runtime::Executor,
    serverpb::v1::{raft_server::RaftServer, NodeIdent, ReplicaLocalState},
    Error, Result, Server,
};

pub const REPLICA_PER_GROUP: usize = 3;

// TODO(walter) root cluster and first replica id.
pub const ROOT_GROUP_ID: u64 = 0;
pub const FIRST_REPLICA_ID: u64 = 1;
pub const FIRST_NODE_ID: u64 = 0;
pub const INITIAL_EPOCH: u64 = 0;

lazy_static::lazy_static! {
    pub static ref SHARD_MIN: Vec<u8> = vec![];
    pub static ref SHARD_MAX: Vec<u8> = vec![];
}

/// The main entrance of engula server.
pub fn run(
    executor: Executor,
    path: PathBuf,
    addr: String,
    init: bool,
    join_list: Vec<String>,
) -> Result<()> {
    let db_path = path.join("db");
    let log_path = path.join("log");
    let raw_db = Arc::new(open_engine(db_path)?);
    let state_engine = StateEngine::new(raw_db.clone())?;

    let router = create_router(&executor, init, addr.clone(), join_list.clone());
    let address_resolver = Arc::new(AddressResolver::new(router.clone()));

    let node = Node::new(
        log_path,
        raw_db,
        state_engine,
        executor.clone(),
        address_resolver.clone(),
        router,
    )?;

    let (_node_id, root) = executor.block_on(async {
        let ident = bootstrap_or_join_cluster(&node, &addr, init, join_list).await?;
        node.bootstrap(&ident).await?;
        let mut root = Root::new(executor.clone(), &ident, addr.to_owned());
        root.bootstrap(&node).await?;
        Ok::<(u64, Root), Error>((ident.node_id, root))
    })?;

    let server = Server {
        node: Arc::new(node),
        root,
        address_resolver,
    };
    let handle = executor.spawn(None, crate::runtime::TaskPriority::High, async move {
        bootstrap_services(&addr, server).await
    });

    executor.block_on(handle)
}

fn create_router(
    executor: &Executor,
    init: bool,
    local_addr: String,
    join_list: Vec<String>,
) -> Router {
    executor.block_on(async { Router::new(if init { vec![local_addr] } else { join_list }).await })
}

/// Listen and serve incoming rpc requests.
async fn bootstrap_services(addr: &str, server: Server) -> Result<()> {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use crate::service::admin::make_admin_service;

    let listener = TcpListener::bind(addr).await?;
    let listener = TcpListenerStream::new(listener);

    Server::builder()
        .add_service(NodeServer::new(server.clone()))
        .add_service(RaftServer::new(server.clone()))
        .add_service(RootServer::new(server.clone()))
        .add_service(make_admin_service())
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
) -> Result<NodeIdent> {
    let state_engine = node.state_engine();
    if let Some(node_ident) = state_engine.read_ident().await? {
        info!(
            "both cluster and node are initialized, node id {}",
            node_ident.node_id
        );
        node.reload_root_from_engine().await?;
        return Ok(node_ident);
    }

    Ok(if init {
        bootstrap_cluster(node, addr).await?
    } else {
        try_join_cluster(node, addr, join_list).await?
    })
}

async fn try_join_cluster(
    node: &Node,
    local_addr: &str,
    join_list: Vec<String>,
) -> Result<NodeIdent> {
    info!("try join a bootstrapted cluster");

    let join_list = join_list
        .into_iter()
        .filter(|addr| *addr != local_addr)
        .collect::<Vec<_>>();

    if join_list.is_empty() {
        return Err(Error::InvalidArgument(
            "the filtered join list is empty".into(),
        ));
    }

    let capacity = NodeCapacity {
        cpu_nums: num_cpus::get() as f64,
        ..Default::default()
    };

    let req = JoinNodeRequest {
        addr: local_addr.to_owned(),
        capacity: Some(capacity),
    };

    let mut backoff: u64 = 1;
    loop {
        match RootClient::connect(join_list.clone()).await {
            Ok(client) => match client.join_node(req.clone()).await {
                Ok(res) => {
                    debug!("issue join request to root server success");
                    let node_ident =
                        save_node_ident(node.state_engine(), res.cluster_id, res.node_id).await;
                    node.update_root(res.roots).await?;
                    return node_ident;
                }
                Err(e) => {
                    warn!(err = ?e, join_list = ?join_list, "failed to join cluster");
                }
            },
            Err(e) => {
                warn!(err = ?e, join_list = ?join_list, "cannot connect to root server");
            }
        }
        std::thread::sleep(Duration::from_secs(backoff));
        backoff = std::cmp::min(backoff * 2, 120);
    }
}

pub(crate) async fn bootstrap_cluster(node: &Node, addr: &str) -> Result<NodeIdent> {
    info!("'--init' is specified, try bootstrap cluster");

    // TODO(walter) clean staled data in db.
    write_initial_cluster_data(node, addr).await?;

    // The first replica in the cluster has been created. It has only one member
    // and does not need to wait for other replicas, so it can directly enter the
    // normal state.
    let state_engine = node.state_engine();
    state_engine
        .save_replica_state(ROOT_GROUP_ID, FIRST_REPLICA_ID, ReplicaLocalState::Normal)
        .await?;

    let cluster_id = vec![];

    let ident = save_node_ident(state_engine, cluster_id.to_owned(), FIRST_NODE_ID).await?;

    info!("bootstrap cluster successfully");

    Ok(ident)
}

async fn save_node_ident(
    state_engine: &StateEngine,
    cluster_id: Vec<u8>,
    node_id: u64,
) -> Result<NodeIdent> {
    let node_ident = NodeIdent {
        cluster_id,
        node_id,
    };
    state_engine.save_ident(&node_ident).await?;

    info!("save node ident, node id {}", node_id);

    Ok(node_ident)
}

async fn write_initial_cluster_data(node: &Node, addr: &str) -> Result<()> {
    // Create the first raft group of cluster, this node is the only member of the raft group.
    let (shards, _) = Schema::init_shards();

    let group = GroupDesc {
        id: ROOT_GROUP_ID,
        epoch: INITIAL_EPOCH,
        shards,
        replicas: vec![ReplicaDesc {
            id: FIRST_REPLICA_ID,
            node_id: FIRST_NODE_ID,
            role: ReplicaRole::Voter.into(),
        }],
    };
    node.create_replica(FIRST_REPLICA_ID, group).await?;

    let root_node = NodeDesc {
        id: FIRST_NODE_ID,
        addr: addr.to_owned(),
        ..Default::default()
    };
    node.update_root(vec![root_node]).await?;

    Ok(())
}
