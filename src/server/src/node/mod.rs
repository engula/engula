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

pub mod group_engine;
pub mod replica;
pub mod resolver;
pub mod route_table;
pub mod state_engine;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use engula_api::server::v1::{GroupDesc, NodeDesc, ReplicaDesc};
use futures::lock::Mutex;
use tracing::{debug, info};

use self::replica::raft::AddressResolver;
pub use self::{
    group_engine::GroupEngine,
    replica::{
        raft::{RaftManager, TransportManager},
        Replica,
    },
    route_table::{RaftRouteTable, ReplicaRouteTable},
    state_engine::StateEngine,
};
use crate::{
    runtime::Executor,
    serverpb::v1::{NodeIdent, ReplicaState},
    Error, Result,
};

#[derive(Clone)]
struct ReplicaInfo {
    group_id: u64,
    state: ReplicaState,
}

/// A structure holds the states of node. Eg create replica.
#[derive(Default)]
struct NodeState
where
    Self: Send,
{
    ident: Option<NodeIdent>,
    replicas: HashMap<u64, ReplicaInfo>,
    root: Vec<NodeDesc>,
}

#[derive(Clone)]
pub struct Node
where
    Self: Send + Sync,
{
    raw_db: Arc<rocksdb::DB>,
    executor: Executor,
    state_engine: StateEngine,
    raft_route_table: RaftRouteTable,
    replica_route_table: ReplicaRouteTable,

    raft_mgr: RaftManager,

    /// `NodeState` of this node, the lock is used to ensure serialization of create/terminate
    /// replica operations.
    node_state: Arc<Mutex<NodeState>>,
}

impl Node {
    pub fn new(
        log_path: PathBuf,
        raw_db: Arc<rocksdb::DB>,
        state_engine: StateEngine,
        executor: Executor,
        address_resolver: Arc<dyn AddressResolver>,
    ) -> Result<Self> {
        let raft_route_table = RaftRouteTable::new();
        let trans_mgr =
            TransportManager::build(executor.clone(), address_resolver, raft_route_table.clone());
        let raft_mgr = RaftManager::open(log_path, executor.clone(), trans_mgr)?;
        Ok(Node {
            raw_db,
            executor,
            state_engine,
            raft_route_table,
            replica_route_table: ReplicaRouteTable::new(),
            raft_mgr,
            node_state: Arc::new(Mutex::new(NodeState::default())),
        })
    }

    pub async fn set_node_ident(&self, node_ident: &NodeIdent) {
        let mut node_state = self.node_state.lock().await;
        node_state.ident = Some(node_ident.to_owned());
    }

    pub async fn recover(&self) -> Result<()> {
        let mut node_state = self.node_state.lock().await;
        debug_assert!(
            node_state.replicas.is_empty(),
            "some replicas are serving before recovery?"
        );

        let it = self.state_engine.iterate_replica_states().await;
        for (group_id, replica_id, state) in it {
            let desc = ReplicaDesc {
                id: replica_id,
                node_id: node_state
                    .ident
                    .as_ref()
                    .expect("node should be bootstrap")
                    .node_id,
                ..Default::default()
            };
            if self
                .start_replica_with_state(group_id, desc, state)
                .await?
                .is_none()
            {
                panic!(
                    "metadata is inconsistent, group {} replica {} not exists",
                    group_id, replica_id
                );
            };
            let replica_info = ReplicaInfo { group_id, state };
            node_state.replicas.insert(replica_id, replica_info);
        }

        Ok(())
    }

    /// Create a replica.
    ///
    /// The replica state is determined by the `GroupDesc`.
    ///
    /// NOTE: This function is idempotent.
    pub async fn create_replica(
        &self,
        replica_id: u64,
        group: GroupDesc,
        recovered: bool,
    ) -> Result<()> {
        let mut node_state = self.node_state.lock().await;
        if node_state.replicas.contains_key(&replica_id) {
            debug!(replica = replica_id, "replica already exists");
            return Ok(());
        }

        // To ensure crash-recovery consistency, first create raft metadata, and then create group
        // metadata. In this way, even if the group is restarted before the group is successfully
        // created, a replica can be recreated by retrying.
        let group_id = group.id;
        Replica::create(replica_id, &group, &self.raft_mgr).await?;
        GroupEngine::create(self.raw_db.clone(), &group).await?;
        let replica_state = if group.replicas.is_empty() {
            ReplicaState::Pending
        } else {
            ReplicaState::Initial
        };
        self.state_engine
            .save_replica_state(group_id, replica_id, replica_state)
            .await?;

        info!(
            "create replica {} of group {} and write initial state success",
            replica_id, group_id
        );

        // If this node has not completed initialization, then there is no need to record
        // `ReplicaInfo`. Because the recovery operation will be performed later, `ReplicaMeta` will
        // be read again and the corresponding `ReplicaInfo` will be created.
        if recovered {
            let replica_info = ReplicaInfo {
                group_id,
                state: replica_state,
            };
            node_state.replicas.insert(replica_id, replica_info);
        }

        Ok(())
    }

    /// Terminate specified replica.
    #[allow(unused)]
    pub async fn terminate_replica(&self, replica_id: u64) -> Result<()> {
        todo!()
    }

    /// Open replica and start serving raft requests.
    pub async fn start_replica(&self, replica_id: u64) -> Result<Option<()>> {
        let node_state = self.node_state.lock().await;
        let info = match node_state.replicas.get(&replica_id) {
            Some(info) => info.clone(),
            None => return Ok(None),
        };

        let desc = ReplicaDesc {
            id: replica_id,
            node_id: node_state
                .ident
                .as_ref()
                .expect("node should be bootstrap")
                .node_id,
            ..Default::default()
        };

        self.start_replica_with_state(info.group_id, desc, info.state)
            .await?
            .expect("replica state exists but group are missed?");

        Ok(Some(()))
    }

    /// Open, recover replica and start serving.
    async fn start_replica_with_state(
        &self,
        group_id: u64,
        desc: ReplicaDesc,
        _state: ReplicaState,
    ) -> Result<Option<()>> {
        let group_engine = match GroupEngine::open(group_id, self.raw_db.clone()).await? {
            Some(group_engine) => group_engine,
            None => return Ok(None),
        };

        let replica_id = desc.id;
        let replica = Replica::recover(group_id, desc, group_engine, &self.raft_mgr).await?;
        let raft_node = replica.raft_node();
        self.replica_route_table.update(Arc::new(replica));
        self.raft_route_table.update(replica_id, raft_node);

        // TODO(walter) start replica workers.

        Ok(Some(()))
    }

    /// Get root addrs that known by node.
    pub async fn get_root(&self) -> Vec<String> {
        let nodes = self.node_state.lock().await.root.to_owned();
        nodes.iter().map(|n| n.addr.to_owned()).collect()
    }

    // Update recent known root nodes.
    pub async fn update_root(&self, roots: Vec<NodeDesc>) -> Result<()> {
        self.state_engine().save_root_nodes(roots).await?;
        self.reload_root_from_engine().await
    }

    pub async fn reload_root_from_engine(&self) -> Result<()> {
        let nodes = self
            .state_engine()
            .load_root_nodes()
            .await?
            .ok_or_else(|| Error::InvalidData("root not found".into()))?;
        self.node_state.lock().await.root = nodes;
        Ok(())
    }

    #[inline]
    pub fn replica_table(&self) -> &ReplicaRouteTable {
        &self.replica_route_table
    }

    #[inline]
    pub fn raft_route_table(&self) -> &RaftRouteTable {
        &self.raft_route_table
    }

    #[inline]
    pub fn state_engine(&self) -> &StateEngine {
        &self.state_engine
    }

    #[inline]
    pub fn executor(&self) -> &Executor {
        &self.executor
    }
}

#[cfg(test)]
mod tests {
    use engula_api::server::v1::{ReplicaDesc, ReplicaRole};
    use tempdir::TempDir;

    use super::*;
    use crate::runtime::ExecutorOwner;
    use crate::bootstrap::INITIAL_EPOCH;

    fn create_node(executor: Executor) -> Node {
        let tmp_dir = TempDir::new("engula").unwrap().into_path();
        let db_dir = tmp_dir.join("db");
        let log_dir = tmp_dir.join("log");

        use crate::bootstrap::open_engine;

        let db = open_engine(db_dir).unwrap();
        let db = Arc::new(db);
        let state_engine = StateEngine::new(db.clone()).unwrap();
        let address_resolver = Arc::new(crate::node::resolver::AddressResolver::new(vec![]));
        Node::new(log_dir, db, state_engine, executor, address_resolver).unwrap()
    }

    async fn replica_state(node: Node, replica_id: u64) -> Option<ReplicaState> {
        node.state_engine()
            .iterate_replica_states()
            .await
            .filter(|(_, id, _)| *id == replica_id)
            .map(|(_, _, state)| state)
            .next()
    }

    #[test]
    fn create_pending_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let node = create_node(executor.clone());

        let group_id = 2;
        let replica_id = 2;
        let group = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas: vec![],
        };

        executor.block_on(async {
            node.create_replica(replica_id, group, false).await.unwrap();

            assert!(matches!(
                replica_state(node, replica_id).await,
                Some(ReplicaState::Pending),
            ));
        });
    }

    #[test]
    fn create_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let node = create_node(executor.clone());

        let group_id = 2;
        let replica_id = 2;
        let group = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas: vec![ReplicaDesc {
                id: replica_id,
                node_id: 1,
                role: ReplicaRole::Voter.into(),
            }],
        };

        executor.block_on(async {
            node.create_replica(replica_id, group, false).await.unwrap();

            assert!(matches!(
                replica_state(node, replica_id).await,
                Some(ReplicaState::Initial),
            ));
        });
    }

    #[test]
    fn recover() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let node = create_node(executor.clone());

        let group_id = 2;
        let replica_id = 2;
        let group = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas: vec![],
        };

        executor.block_on(async {
            node.create_replica(replica_id, group, false).await.unwrap();
        });

        drop(node);
        let node = create_node(executor.clone());
        executor.block_on(async {
            node.recover().await.unwrap();
        })
    }
}
