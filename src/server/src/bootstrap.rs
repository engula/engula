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

use std::{path::Path, sync::Arc, time::Duration, vec};

use engula_api::server::v1::{node_server::NodeServer, root_server::RootServer, *};
use engula_client::{ConnManager, RootClient, Router};
use tracing::{debug, info, warn};

use crate::{
    discovery::RootDiscovery,
    node::{engine::StateEngine, resolver::AddressResolver, Node},
    root::{Root, Schema},
    runtime::{Executor, Shutdown},
    serverpb::v1::{raft_server::RaftServer, NodeIdent, ReplicaLocalState},
    Config, Error, Provider, Result, Server,
};

pub const REPLICA_PER_GROUP: usize = 3;

pub const ROOT_GROUP_ID: u64 = 0;
pub const INIT_USER_GROUP_ID: u64 = ROOT_GROUP_ID + 1;
pub const FIRST_REPLICA_ID: u64 = 1;
pub const INIT_USER_REPLICA_ID: u64 = FIRST_REPLICA_ID + 1;
pub const FIRST_NODE_ID: u64 = 0;
pub const INITIAL_EPOCH: u64 = 0;
pub const INITAL_JOB_ID: u64 = 0;

lazy_static::lazy_static! {
    pub static ref SHARD_MIN: Vec<u8> = vec![];
    pub static ref SHARD_MAX: Vec<u8> = vec![];
}

/// The main entrance of engula server.
pub fn run(config: Config, executor: Executor, shutdown: Shutdown) -> Result<()> {
    executor.block_on(async {
        let provider = build_provider(&config, executor.clone()).await?;
        let node = Node::new(config.clone(), provider.clone())?;

        let ident = bootstrap_or_join_cluster(&config, &node, &provider.root_client).await?;
        node.bootstrap(&ident).await?;
        let root = Root::new(provider.clone(), &ident, config.clone());
        let initial_node_descs = root.bootstrap(&node).await?;
        provider
            .address_resolver
            .set_initial_nodes(initial_node_descs);

        info!("node {} starts serving requests", ident.node_id);

        let server = Server {
            node: Arc::new(node),
            root,
            address_resolver: provider.address_resolver.clone(),
        };
        bootstrap_services(&config.addr, server, shutdown).await
    })
}

/// Listen and serve incoming rpc requests.
async fn bootstrap_services(addr: &str, server: Server, shutdown: Shutdown) -> Result<()> {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use crate::service::admin::make_admin_service;

    let listener = TcpListener::bind(addr).await?;
    let listener = TcpListenerStream::new(listener);

    let server = Server::builder()
        .accept_http1(true) // Support http1 for admin service.
        .add_service(NodeServer::new(server.clone()))
        .add_service(RaftServer::new(server.clone()))
        .add_service(RootServer::new(server.clone()))
        .add_service(make_admin_service(server.clone()))
        .serve_with_incoming(listener);

    crate::runtime::select! {
        res = server => { res? }
        _ = shutdown => {}
    };

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
    config: &Config,
    node: &Node,
    root_client: &RootClient,
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

    Ok(if config.init {
        bootstrap_cluster(node, &config.addr).await?
    } else {
        try_join_cluster(
            node,
            &config.addr,
            config.join_list.clone(),
            config.cpu_nums,
            root_client,
        )
        .await?
    })
}

async fn try_join_cluster(
    node: &Node,
    local_addr: &str,
    join_list: Vec<String>,
    cpu_nums: u32,
    root_client: &RootClient,
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

    let cpu_nums = if cpu_nums == 0 {
        num_cpus::get() as f64
    } else {
        cpu_nums as f64
    };
    let capacity = NodeCapacity {
        cpu_nums,
        ..Default::default()
    };

    let req = JoinNodeRequest {
        addr: local_addr.to_owned(),
        capacity: Some(capacity),
    };

    let mut backoff: u64 = 1;
    loop {
        match root_client.join_node(req.clone()).await {
            Ok(res) => {
                debug!("issue join request to root server success");
                let node_ident =
                    save_node_ident(node.state_engine(), res.cluster_id, res.node_id).await;
                node.update_root(res.root.unwrap_or_default()).await?;
                return node_ident;
            }
            Err(e) => {
                warn!(err = ?e, join_list = ?join_list, "failed to join cluster");
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
    state_engine
        .save_replica_state(
            INIT_USER_GROUP_ID,
            INIT_USER_REPLICA_ID,
            ReplicaLocalState::Normal,
        )
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

    // Create another group with empty shard to prepare user usage.
    let init_group = GroupDesc {
        id: INIT_USER_GROUP_ID,
        epoch: INITIAL_EPOCH,
        shards: vec![],
        replicas: vec![ReplicaDesc {
            id: INIT_USER_REPLICA_ID,
            node_id: FIRST_NODE_ID,
            role: ReplicaRole::Voter.into(),
        }],
    };
    node.create_replica(INIT_USER_REPLICA_ID, init_group)
        .await?;

    let root_node = NodeDesc {
        id: FIRST_NODE_ID,
        addr: addr.to_owned(),
        ..Default::default()
    };
    let root_desc = RootDesc {
        epoch: INITIAL_EPOCH,
        root_nodes: vec![root_node],
    };
    node.update_root(root_desc).await?;

    Ok(())
}

pub(crate) async fn build_provider(config: &Config, executor: Executor) -> Result<Arc<Provider>> {
    let db_path = config.root_dir.join("db");
    let log_path = config.root_dir.join("log");
    let raw_db = Arc::new(open_engine(&db_path)?);

    let root_list = if config.init {
        vec![config.addr.clone()]
    } else {
        config.join_list.clone()
    };
    let state_engine = StateEngine::new(raw_db.clone())?;
    let discovery = Arc::new(RootDiscovery::new(root_list, state_engine.clone()));
    let conn_manager = ConnManager::new();
    let root_client = RootClient::new(discovery, conn_manager.clone());
    let router = Router::new(root_client.clone()).await;
    let address_resolver = Arc::new(AddressResolver::new(router.clone()));
    let provider = Arc::new(Provider {
        log_path,
        db_path,
        conn_manager,
        root_client,
        router,
        address_resolver,
        raw_db,
        state_engine,
        executor,
    });
    Ok(provider)
}
