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

mod allocator;
mod job;
mod schema;
mod store;
mod watch;

use std::{
    collections::{hash_map, HashMap},
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use engula_api::{
    server::v1::{report_request::GroupUpdates, watch_response::*, *},
    v1::{
        collection_desc as co_desc, create_collection_request as co_req, CollectionDesc,
        DatabaseDesc,
    },
};
use engula_client::NodeClient;
use tracing::{info, warn};

pub(crate) use self::schema::*;
pub use self::watch::{WatchHub, Watcher, WatcherInitializer};
use self::{allocator::SysAllocSource, schema::ReplicaNodes, store::RootStore};
use crate::{
    bootstrap::{INITIAL_EPOCH, REPLICA_PER_GROUP, SHARD_MAX, SHARD_MIN},
    node::{Node, Replica, ReplicaRouteTable},
    runtime::{Executor, TaskPriority},
    serverpb::v1::NodeIdent,
    Error, Result,
};

#[derive(Clone)]
pub struct Root {
    shared: Arc<RootShared>,
    alloc: allocator::Allocator<SysAllocSource>,
}

pub struct RootShared {
    executor: Executor,
    node_ident: NodeIdent,
    local_addr: String,
    core: Mutex<Option<RootCore>>,
    watcher_hub: Arc<WatchHub>,
}

impl RootShared {
    pub fn schema(&self) -> Result<Arc<Schema>> {
        let core = self.core.lock().unwrap();
        core.as_ref()
            .map(|c| c.schema.clone())
            .ok_or_else(|| Error::NotRootLeader(vec![]))
    }
}

struct RootCore {
    schema: Arc<Schema>,
}

impl Root {
    pub fn new(executor: Executor, node_ident: &NodeIdent, local_addr: String) -> Self {
        let shared = Arc::new(RootShared {
            executor,
            local_addr,
            core: Mutex::new(None),
            node_ident: node_ident.to_owned(),
            watcher_hub: Default::default(),
        });
        let info = Arc::new(SysAllocSource::new(shared.clone()));
        let alloc = allocator::Allocator::new(info, REPLICA_PER_GROUP);
        Self { alloc, shared }
    }

    pub fn is_root(&self) -> bool {
        self.shared.core.lock().unwrap().is_some()
    }

    pub fn current_node_id(&self) -> u64 {
        self.shared.node_ident.node_id
    }

    pub async fn bootstrap(&mut self, node: &Node) -> Result<()> {
        let replica_table = node.replica_table().clone();
        let root = self.clone();
        self.shared
            .executor
            .spawn(None, TaskPriority::Middle, async move {
                root.run(replica_table).await;
            });
        Ok(())
    }

    pub fn schema(&self) -> Result<Arc<Schema>> {
        self.shared.schema()
    }

    pub fn watcher_hub(&self) -> Arc<WatchHub> {
        self.shared.watcher_hub.clone()
    }

    async fn run(&self, replica_table: ReplicaRouteTable) -> ! {
        let mut bootstrapped = false;
        loop {
            let root_replica = self.fetch_root_replica(&replica_table).await;

            // Wait the current root replica becomes a leader.
            if root_replica.on_leader(false).await.is_ok() {
                match self
                    .step_leader(&self.shared.local_addr, root_replica, &mut bootstrapped)
                    .await
                {
                    Ok(()) | Err(Error::NotLeader(_, _)) => {
                        // Step follower
                        continue;
                    }
                    Err(err) => {
                        todo!("handle error: {}", err)
                    }
                }
            }
        }
    }

    async fn fetch_root_replica(&self, replica_table: &ReplicaRouteTable) -> Arc<Replica> {
        use futures::future::poll_fn;
        poll_fn(
            |ctx| match replica_table.current_root_replica(Some(ctx.waker().clone())) {
                Some(root_replica) => Poll::Ready(root_replica),
                None => Poll::Pending,
            },
        )
        .await
    }

    async fn step_leader(
        &self,
        local_addr: &str,
        root_replica: Arc<Replica>,
        bootstrapped: &mut bool,
    ) -> Result<()> {
        let store = Arc::new(RootStore::new(root_replica.to_owned()));
        let mut schema = Schema::new(store.clone());

        // Only when the program is initialized is it checked for bootstrap, after which the
        // leadership change does not need to check for whether bootstrap or not.
        if !*bootstrapped {
            schema
                .try_bootstrap(local_addr, self.shared.node_ident.cluster_id.clone())
                .await?;
            *bootstrapped = true;
        }

        {
            let mut core = self.shared.core.lock().unwrap();
            *core = Some(RootCore {
                schema: Arc::new(schema.to_owned()),
            });
        }

        info!("step root service leader");

        while let Ok(Some(_)) = root_replica.to_owned().on_leader(true).await {
            if let Err(err) = self.send_heartbeat(schema.to_owned()).await {
                warn!("send heartbeat fatal: {}", err);
                break;
            }

            if let Err(err) = self.reconcile_group().await {
                warn!("reconcile group fatal: {}", err);
                break;
            }

            crate::runtime::time::sleep(Duration::from_secs(1)).await;
        }
        info!("current root node drop leader");

        // After that, RootCore needs to be set to None before returning.
        {
            let mut core = self.shared.core.lock().unwrap();
            *core = None;
        }

        Ok(())
    }
}

impl Root {
    pub async fn create_database(&self, name: String) -> Result<DatabaseDesc> {
        let desc = self
            .schema()?
            .create_database(DatabaseDesc {
                name,
                ..Default::default()
            })
            .await?;
        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Database(desc.to_owned())),
            }])
            .await;
        Ok(desc)
    }

    pub async fn delete_database(&self, name: &str) -> Result<()> {
        let id = self.schema()?.delete_database(name).await?;
        self.watcher_hub()
            .notify_deletes(vec![DeleteEvent {
                event: Some(delete_event::Event::Database(id)),
            }])
            .await;
        Ok(())
    }

    pub async fn create_collection(
        &self,
        name: String,
        database: String,
        partition: Option<co_req::Partition>,
    ) -> Result<CollectionDesc> {
        let schema = self.schema()?;
        let db = schema.get_database(&database).await?;
        if db.is_none() {
            return Err(Error::DatabaseNotFound(database));
        }
        let collection = schema
            .create_collection(CollectionDesc {
                name,
                db: db.unwrap().id,
                partition: partition.map(|p| match p {
                    co_req::Partition::Hash(hash) => {
                        co_desc::Partition::Hash(co_desc::HashPartition { slots: hash.slots })
                    }
                    co_req::Partition::Range(_) => {
                        co_desc::Partition::Range(co_desc::RangePartition {})
                    }
                }),
                ..Default::default()
            })
            .await?;

        // TODO: compensating task to cleanup shard create success but batch_write failure(maybe in
        // handle hearbeat resp).
        self.create_collection_shard(schema.to_owned(), collection.to_owned())
            .await?;

        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Collection(collection.to_owned())),
            }])
            .await;

        Ok(collection)
    }

    async fn create_collection_shard(
        &self,
        schema: Arc<Schema>,
        collection: CollectionDesc,
    ) -> Result<()> {
        let partition =
            collection
                .partition
                .unwrap_or(co_desc::Partition::Hash(co_desc::HashPartition {
                    slots: 1,
                }));

        let partitions = match partition {
            co_desc::Partition::Hash(hash_partition) => {
                let mut ps = Vec::with_capacity(hash_partition.slots as usize);
                for id in 0..hash_partition.slots {
                    ps.push(shard_desc::Partition::Hash(shard_desc::HashPartition {
                        slot_id: id as u32,
                        slots: hash_partition.slots.to_owned(),
                    }));
                }
                ps
            }
            co_desc::Partition::Range(_) => {
                vec![shard_desc::Partition::Range(shard_desc::RangePartition {
                    start: SHARD_MIN.to_owned(),
                    end: SHARD_MAX.to_owned(),
                })]
            }
        };

        let candiate_groups = self.alloc.place_group_for_shard(partitions.len()).await?;
        assert!(!candiate_groups.is_empty());

        let mut group_shards: HashMap<u64, Vec<ShardDesc>> = HashMap::new();
        for (group_idx, partition) in partitions.into_iter().enumerate() {
            let id = schema.next_shard_id().await?;
            let shard = ShardDesc {
                id,
                collection_id: collection.id.to_owned(),
                partition: Some(partition),
            };
            let group = candiate_groups
                .get(group_idx % candiate_groups.len())
                .unwrap();
            match group_shards.entry(group.id.to_owned()) {
                hash_map::Entry::Occupied(mut ent) => {
                    ent.get_mut().push(shard);
                }
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(vec![shard]);
                }
            }
        }

        for (group_id, descs) in group_shards {
            schema.create_shards(group_id, descs).await?;
        }

        Ok(())
    }

    pub async fn delete_collection(&self, name: &str, database: &str) -> Result<()> {
        let schema = self.schema()?;
        let collection = schema.get_collection(database, name).await?;
        if let Some(collection) = collection {
            let id = collection.id;
            schema.delete_collection(collection).await?;
            self.watcher_hub()
                .notify_deletes(vec![DeleteEvent {
                    event: Some(delete_event::Event::Collection(id)),
                }])
                .await;
        }
        Ok(())
    }

    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseDesc>> {
        self.schema()?.get_database(name).await
    }

    pub async fn get_collection(
        &self,
        name: &str,
        database: &str,
    ) -> Result<Option<CollectionDesc>> {
        self.schema()?.get_collection(database, name).await
    }

    pub async fn watch(&self, cur_groups: HashMap<u64, u64>) -> Result<Watcher> {
        let schema = self.schema()?;

        let watcher = {
            let hub = self.watcher_hub();
            let (watcher, mut initializer) = hub.create_watcher().await;
            let (updates, deletes) = schema.list_all_events(cur_groups).await?;
            initializer.set_init_resp(updates, deletes);
            watcher
        };
        Ok(watcher)
    }

    pub async fn join(
        &self,
        addr: String,
        capacity: NodeCapacity,
    ) -> Result<(Vec<u8>, NodeDesc, ReplicaNodes)> {
        let schema = self.schema()?;
        let node = schema
            .add_node(NodeDesc {
                addr,
                capacity: Some(capacity),
                ..Default::default()
            })
            .await?;
        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Node(node.to_owned())),
            }])
            .await;

        let cluster_id = schema.cluster_id().await?.unwrap();
        let mut roots = schema.get_root_replicas().await?;
        roots.move_first(node.id);
        Ok((cluster_id, node, roots))
    }

    pub async fn report(&self, updates: Vec<GroupUpdates>) -> Result<()> {
        // mock report doesn't work.
        // return Ok(());

        let schema = self.schema()?;
        let mut update_events = Vec::new();
        let mut changed_group_states = Vec::new();
        for u in updates {
            if u.group_desc.is_some() {
                // TODO: check & handle remove replicas from group
            }
            schema
                .update_group_replica(u.group_desc.to_owned(), u.replica_state.to_owned())
                .await?;
            if let Some(desc) = u.group_desc {
                update_events.push(UpdateEvent {
                    event: Some(update_event::Event::Group(desc)),
                })
            }
            if let Some(state) = u.replica_state {
                changed_group_states.push(state.group_id);
            }
        }

        let mut states = schema.list_group_state().await?; // TODO: fix poor performance.
        states.retain(|s| changed_group_states.contains(&s.group_id));
        for state in states {
            update_events.push(UpdateEvent {
                event: Some(update_event::Event::GroupState(state)),
            })
        }

        self.watcher_hub().notify_updates(update_events).await;

        Ok(())
    }

    async fn create_groups(&self, cnt: usize) -> Result<()> {
        for _ in 0..cnt {
            let nodes = self
                .alloc
                .allocate_group_replica(vec![], REPLICA_PER_GROUP as usize)
                .await?;
            self.create_group(nodes).await?;
        }
        Ok(())
    }

    async fn create_group(&self, nodes: Vec<NodeDesc>) -> Result<()> {
        let schema = self.schema()?;
        let group_id = schema.next_group_id().await?;
        let mut replicas = Vec::new();
        let mut node_to_replica = HashMap::new();
        for n in &nodes {
            let replica_id = schema.next_replica_id().await?;
            replicas.push(ReplicaDesc {
                id: replica_id,
                node_id: n.id,
                role: ReplicaRole::Voter.into(),
            });
            node_to_replica.insert(n.id, replica_id);
        }
        let group_tmpl = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas,
        };
        for n in &nodes {
            let replica_id = node_to_replica.get(&n.id).unwrap();
            Self::try_create_replica(&n.addr, replica_id, group_tmpl.clone()).await?
        }
        // TODO(zojw): rety and cancel all logic.
        Ok(())
    }

    async fn try_create_replica(addr: &str, replica_id: &u64, group: GroupDesc) -> Result<()> {
        let node_client = NodeClient::connect(addr.to_owned()).await?;
        node_client
            .create_replica(replica_id.to_owned(), group)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod root_test {

    use std::sync::Arc;

    use engula_api::{
        server::v1::watch_response::{update_event, UpdateEvent},
        v1::DatabaseDesc,
    };
    use engula_client::Router;
    use futures::StreamExt;
    use tempdir::TempDir;

    use crate::{
        bootstrap::bootstrap_cluster,
        node::{Node, StateEngine},
        root::Root,
        runtime::{Executor, ExecutorOwner},
        serverpb::v1::NodeIdent,
    };

    fn create_root(executor: Executor, node_ident: &NodeIdent) -> Root {
        Root::new(executor, node_ident, "0.0.0.0:8888".into())
    }

    fn create_node(executor: Executor) -> Node {
        let tmp_dir = TempDir::new("engula").unwrap().into_path();
        let db_dir = tmp_dir.join("db");
        let log_dir = tmp_dir.join("log");

        use crate::bootstrap::open_engine;

        let db = open_engine(db_dir).unwrap();
        let db = Arc::new(db);
        let state_engine = StateEngine::new(db.clone()).unwrap();
        let address_resolver = Arc::new(crate::node::resolver::AddressResolver::new(vec![]));
        let router = executor.block_on(async { Router::new("".to_owned()).await });
        Node::new(
            log_dir,
            db,
            state_engine,
            executor,
            address_resolver,
            router,
        )
        .unwrap()
    }

    #[test]
    fn boostrap_root() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();

        let ident = NodeIdent {
            cluster_id: vec![],
            node_id: 1,
        };
        let node = create_node(executor.to_owned());
        let mut root = create_root(executor.to_owned(), &ident);

        executor.block_on(async {
            bootstrap_cluster(&node, "0.0.0.0:8888").await.unwrap();
            node.bootstrap(&ident).await.unwrap();
            root.bootstrap(&node).await.unwrap();
            // TODO: test on leader logic later.
        });
    }

    #[test]
    fn watch_hub() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();

        let ident = NodeIdent {
            cluster_id: vec![],
            node_id: 1,
        };

        let root = create_root(executor.to_owned(), &ident);
        executor.block_on(async {
            let hub = root.watcher_hub();
            let _create_db1_event = Some(update_event::Event::Database(DatabaseDesc {
                id: 1,
                name: "db1".into(),
            }));
            let mut w = {
                let (w, mut initializer) = hub.create_watcher().await;
                initializer.set_init_resp(
                    vec![UpdateEvent {
                        event: _create_db1_event,
                    }],
                    vec![],
                );
                w
            };
            let resp1 = w.next().await.unwrap().unwrap();
            assert!(matches!(&resp1.updates[0].event, _create_db1_event));

            let mut w2 = {
                let (w, _) = hub.create_watcher().await;
                w
            };

            let _create_db2_event = Some(update_event::Event::Database(DatabaseDesc {
                id: 2,
                name: "db2".into(),
            }));
            hub.notify_updates(vec![UpdateEvent {
                event: _create_db2_event,
            }])
            .await;
            let resp2 = w.next().await.unwrap().unwrap();
            assert!(matches!(&resp2.updates[0].event, _create_db2_event));
            let resp22 = w2.next().await.unwrap().unwrap();
            assert!(matches!(&resp22.updates[0].event, _create_db2_event));
            // hub.notify_error(Error::NotRootLeader(vec![])).await;
        });
    }
}
