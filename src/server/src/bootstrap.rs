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
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    time::Duration,
    vec,
};

use engula_api::server::v1::{
    node_server::NodeServer,
    root_server::RootServer,
    shard_desc::{Partition, RangePartition},
    GroupDesc, JoinNodeRequest, JoinNodeResponse, NodeDesc, ReplicaDesc, ReplicaRole, ShardDesc,
};
use tracing::{debug, info, warn};

use crate::{
    node::{resolver::AddressResolver, state_engine::StateEngine, Node},
    root::Root,
    runtime::Executor,
    serverpb::v1::{raft_server::RaftServer, NodeIdent, ReplicaState},
    Error, Result, Server,
};

// TODO(walter) root cluster and first replica id.
pub const ROOT_GROUP_ID: u64 = 0;
pub const NA_SHARD_ID: u64 = 0;
pub const ROOT_SHARD_ID: u64 = 1;
pub const FIRST_REPLICA_ID: u64 = 1;
pub const FIRST_NODE_ID: u64 = 0;

lazy_static::lazy_static! {
    pub static ref MIN_KEY: Vec<u8> = vec![];
    pub static ref MAX_KEY: Vec<u8> = vec![0xff, 0xff];
}

/// The main entrance of engula server.
pub fn run(
    executor: Executor,
    path: PathBuf,
    addr: String,
    init: bool,
    join_list: Vec<String>,
    socket_addr_sender: Option<mpsc::Sender<SocketAddr>>,
) -> Result<()> {
    let db_path = path.join("db");
    let log_path = path.join("log");
    let raw_db = Arc::new(open_engine(db_path)?);
    let state_engine = StateEngine::new(raw_db.clone())?;
    let address_resolver = Arc::new(AddressResolver::new(join_list.clone()));
    let node = Node::new(
        log_path,
        raw_db,
        state_engine,
        executor.clone(),
        address_resolver.clone(),
    )?;

    let (node_id, root) = executor.block_on(async {
        let ident = bootstrap_or_join_cluster(&node, &addr, init, join_list).await?;
        node.set_node_ident(&ident).await;
        recover_groups(&node).await?;
        let mut root = Root::new(executor.clone(), &ident, addr.to_owned());
        root.bootstrap(&node).await?;
        Ok::<(u64, Root), Error>((ident.node_id, root))
    })?;

    // TODO address_resolver watches node descriptors.
    let node_desc = NodeDesc {
        id: node_id,
        addr: addr.to_owned(),
    };
    address_resolver.insert(&node_desc);

    let server = Server {
        node: Arc::new(node),
        root,
        address_resolver,
    };
    let handle = executor.spawn(None, crate::runtime::TaskPriority::High, async move {
        bootstrap_services(&addr, server, socket_addr_sender).await
    });

    executor.block_on(handle)
}

/// Listen and serve incoming rpc requests.
async fn bootstrap_services(
    addr: &str,
    server: Server,
    socket_addr_sender: Option<mpsc::Sender<SocketAddr>>,
) -> Result<()> {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    let listener = TcpListener::bind(addr).await?;
    if let Some(sender) = socket_addr_sender {
        sender
            .send(listener.local_addr().unwrap())
            .unwrap_or_default();
    }

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
) -> Result<NodeIdent> {
    let state_engine = node.state_engine();
    if let Some(node_ident) = state_engine.read_ident().await? {
        info!(
            "both cluster and node are initialized, node id {}",
            node_ident.node_id
        );
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
        .iter()
        .filter(|addr| *addr != local_addr)
        .collect::<Vec<_>>();

    if join_list.is_empty() {
        return Err(Error::InvalidArgument(
            "the filtered join list is empty".into(),
        ));
    }

    let prev_cluster_id = if let Some(ident) = node.state_engine().read_ident().await? {
        Some(ident.cluster_id)
    } else {
        None
    };

    let mut backoff: u64 = 1;
    loop {
        for addr in &join_list {
            match issue_join_request(addr, local_addr, prev_cluster_id.to_owned()).await {
                Ok(resp) => {
                    let node_ident = save_node_ident(
                        node.state_engine(),
                        resp.cluster_id.to_owned(),
                        resp.node_id,
                    )
                    .await;
                    node.update_root(resp.roots).await?;
                    return node_ident;
                }
                Err(e) => {
                    warn!(err = ?e, root = ?addr, "issue join request to root server");
                }
            }
        }
        std::thread::sleep(Duration::from_secs(backoff));
        backoff = std::cmp::min(backoff, 120);
    }
}

async fn issue_join_request(
    target_addr: &str,
    local_addr: &str,
    cluster_id: Option<Vec<u8>>,
) -> Result<JoinNodeResponse> {
    use engula_api::server::v1::root_client::RootClient;
    use tonic::Request;

    let mut client = RootClient::connect(format!("http://{}", target_addr)).await?;
    let resp = client
        .join(Request::new(JoinNodeRequest {
            addr: local_addr.to_owned(),
            cluster_id,
        }))
        .await?;
    Ok(resp.into_inner())
}

async fn bootstrap_cluster(node: &Node, addr: &str) -> Result<NodeIdent> {
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
    let shard = ShardDesc {
        id: ROOT_SHARD_ID,
        parent_id: NA_SHARD_ID,
        partition: Some(Partition::Range(RangePartition {
            start: MIN_KEY.to_owned(),
            end: MAX_KEY.to_owned(),
        })),
    };

    let group = GroupDesc {
        id: ROOT_GROUP_ID,
        shards: vec![shard],
        replicas: vec![ReplicaDesc {
            id: FIRST_REPLICA_ID,
            node_id: FIRST_NODE_ID,
            role: ReplicaRole::Voter.into(),
        }],
    };
    node.create_replica(FIRST_REPLICA_ID, group, false).await?;

    let root_node = NodeDesc {
        id: FIRST_NODE_ID,
        addr: addr.to_owned(),
    };
    node.update_root(vec![root_node]).await?;

    Ok(())
}

async fn recover_groups(node: &Node) -> Result<()> {
    node.recover().await?;
    Ok(())
}
