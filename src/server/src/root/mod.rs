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
mod liveness;
mod schedule;
mod schema;
mod store;
mod watch;

use std::{
    collections::{hash_map, HashMap, HashSet},
    sync::{Arc, Mutex},
    task::Poll,
};

use engula_api::{
    server::v1::{report_request::GroupUpdates, watch_response::*, *},
    v1::{
        collection_desc as co_desc, create_collection_request as co_req, CollectionDesc,
        DatabaseDesc,
    },
};
use engula_client::NodeClient;
use tracing::{error, info, trace, warn};

pub(crate) use self::schema::*;
pub use self::{
    allocator::AllocatorConfig,
    watch::{WatchHub, Watcher, WatcherInitializer},
};
use self::{
    allocator::SysAllocSource, schedule::ReconcileScheduler, schema::ReplicaNodes, store::RootStore,
};
use crate::{
    bootstrap::{SHARD_MAX, SHARD_MIN},
    node::{Node, Replica, ReplicaRouteTable},
    runtime::TaskPriority,
    serverpb::v1::{
        reconcile_task::Task, CreateCollectionShardStep, CreateCollectionShards, GroupShards,
        NodeIdent, ReconcileTask,
    },
    Config, Error, Provider, Result,
};

#[derive(Clone)]
pub struct Root {
    shared: Arc<RootShared>,
    alloc: Arc<allocator::Allocator<SysAllocSource>>,
    liveness: Arc<liveness::Liveness>,
    scheduler: Arc<ReconcileScheduler>,
}

pub struct RootShared {
    provider: Arc<Provider>,
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
            .ok_or_else(|| Error::NotRootLeader(RootDesc::default(), None))
    }
}

struct RootCore {
    schema: Arc<Schema>,
}

impl Root {
    pub(crate) fn new(provider: Arc<Provider>, node_ident: &NodeIdent, cfg: Config) -> Self {
        let local_addr = cfg.addr.clone();
        let shared = Arc::new(RootShared {
            provider,
            local_addr,
            core: Mutex::new(None),
            node_ident: node_ident.to_owned(),
            watcher_hub: Default::default(),
        });
        let liveness = Arc::new(liveness::Liveness::new(cfg.to_owned()));
        let info = Arc::new(SysAllocSource::new(shared.clone(), liveness.to_owned()));
        let alloc = Arc::new(allocator::Allocator::new(info, cfg.allocator));
        let sched_ctx = schedule::ScheduleContext::new(shared.clone(), alloc.clone());
        let scheduler = Arc::new(schedule::ReconcileScheduler::new(sched_ctx));
        Self {
            alloc,
            shared,
            liveness,
            scheduler,
        }
    }

    pub fn is_root(&self) -> bool {
        self.shared.core.lock().unwrap().is_some()
    }

    pub fn current_node_id(&self) -> u64 {
        self.shared.node_ident.node_id
    }

    pub async fn bootstrap(&self, node: &Node) -> Result<Vec<NodeDesc>> {
        let replica_table = node.replica_table().clone();
        let root = self.clone();
        self.shared
            .provider
            .executor
            .spawn(None, TaskPriority::Middle, async move {
                root.run(replica_table).await;
            });

        if let Some(replica) = node.replica_table().current_root_replica(None) {
            let engine = replica.group_engine();
            Ok(Schema::list_node_raw(engine).await?)
        } else {
            Ok(vec![])
        }
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
            let root_replica = fetch_root_replica(&replica_table).await;

            // Wait the current root replica becomes a leader.
            if let Ok(Some(_)) = root_replica.on_leader("root", false).await {
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
            if let Err(err) = schema
                .try_bootstrap_root(local_addr, self.shared.node_ident.cluster_id.clone())
                .await
            {
                error!(err = ?err, "boostrap error");
                panic!("boostrap cluster failure")
            }
            *bootstrapped = true;
        }

        {
            let mut core = self.shared.core.lock().unwrap();
            *core = Some(RootCore {
                schema: Arc::new(schema.to_owned()),
            });
        }

        let node_id = self.shared.node_ident.node_id;
        info!(
            "node {node_id} step root service leader, heartbeat_interval: {:?}, liveness_threshold: {:?}",
            self.liveness.heartbeat_interval(),
            self.liveness.liveness_threshold,

        );

        while let Ok(Some(_)) = root_replica.to_owned().on_leader("root", true).await {
            if let Err(err) = self.send_heartbeat(Arc::new(schema.to_owned())).await {
                warn!(err = ?err, "send heartbeat meet error");
                if Self::need_drop_root_leader(&err) {
                    break;
                }
            }

            self.scheduler.step_one().await;

            crate::runtime::time::sleep(self.liveness.heartbeat_interval()).await;
        }
        info!("node {node_id} current root node drop leader");

        // After that, RootCore needs to be set to None before returning.
        {
            let mut core = self.shared.core.lock().unwrap();
            *core = None;
        }

        Ok(())
    }

    fn need_drop_root_leader(err: &Error) -> bool {
        matches!(err, Error::Raft(raft::Error::ProposalDropped))
    }

    pub async fn info(&self) -> Result<String> {
        let schema = self.schema()?;
        let nodes = schema.list_node().await?;
        let groups = schema.list_group().await?;
        let replicas = groups
            .iter()
            .flat_map(|g| g.replicas.iter().map(|r| (r, g.id)).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let states = schema.list_replica_state().await?;
        let dbs = schema.list_database().await?;
        let collections = schema.list_collection().await?;

        let balanced = !self.scheduler.need_reconcile().await?;

        use diagnosis::*;

        let info = Metadata {
            nodes: nodes
                .iter()
                .map(|n| Node {
                    id: n.id,
                    addr: n.addr.to_owned(),
                    replicas: replicas
                        .iter()
                        .filter(|(r, _)| r.node_id == n.id)
                        .map(|(r, g)| NodeReplica {
                            id: r.id,
                            group: g.to_owned(),
                            replica_role: r.role,
                            raft_role: states
                                .iter()
                                .find(|s| s.replica_id == r.id)
                                .map(|s| s.role)
                                .unwrap_or(-1),
                        })
                        .collect::<Vec<_>>(),
                })
                .collect::<Vec<_>>(),
            databases: dbs
                .iter()
                .map(|d| Database {
                    id: d.id,
                    name: d.name.to_owned(),
                    collections: collections
                        .iter()
                        .filter(|c| c.db == d.id)
                        .map(|c| {
                            let mode = match c.partition.as_ref().unwrap() {
                                co_desc::Partition::Hash(co_desc::HashPartition { slots }) => {
                                    format!("hash({slots})")
                                }
                                co_desc::Partition::Range(co_desc::RangePartition {}) => {
                                    "range".to_owned()
                                }
                            };
                            Collection {
                                id: c.id,
                                name: c.name.to_owned(),
                                mode,
                            }
                        })
                        .collect::<Vec<_>>(),
                })
                .collect::<Vec<_>>(),
            groups: groups
                .iter()
                .map(|g| Group {
                    id: g.id,
                    epoch: g.epoch,
                    replicas: g
                        .replicas
                        .iter()
                        .map(|r| {
                            let s = states.iter().find(|s| s.replica_id == r.id);
                            GroupReplica {
                                id: r.id,
                                node: r.node_id,
                                replica_role: r.role,
                                raft_role: s.map(|s| s.role).unwrap_or(-1),
                                term: s.map(|s| s.term).unwrap_or(0),
                            }
                        })
                        .collect::<Vec<_>>(),
                    shards: g
                        .shards
                        .iter()
                        .map(|s| {
                            let part = match s.partition.as_ref().unwrap() {
                                shard_desc::Partition::Hash(shard_desc::HashPartition {
                                    slot_id,
                                    slots,
                                }) => {
                                    format!("hash: {slot_id} of {slots}")
                                }
                                shard_desc::Partition::Range(shard_desc::RangePartition {
                                    start,
                                    end,
                                }) => {
                                    format!("range: {start:?} to {end:?}")
                                }
                            };
                            GroupShard {
                                id: s.id,
                                collection: s.collection_id,
                                partition: part,
                            }
                        })
                        .collect::<Vec<_>>(),
                })
                .collect::<Vec<_>>(),
            balanced,
        };
        Ok(serde_json::to_string(&info).unwrap())
    }

    async fn get_node_client(&self, addr: String) -> Result<NodeClient> {
        let client = self
            .shared
            .provider
            .conn_manager
            .get_node_client(addr)
            .await?;
        Ok(client)
    }
}

impl Root {
    pub async fn create_database(&self, name: String) -> Result<DatabaseDesc> {
        let desc = self
            .schema()?
            .create_database(DatabaseDesc {
                name: name.to_owned(),
                ..Default::default()
            })
            .await?;
        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Database(desc.to_owned())),
            }])
            .await;
        trace!(database_id = desc.id, database = ?name, "create database");
        Ok(desc)
    }

    pub async fn delete_database(&self, name: &str) -> Result<()> {
        let id = self.schema()?.delete_database(name).await?;
        self.watcher_hub()
            .notify_deletes(vec![DeleteEvent {
                event: Some(delete_event::Event::Database(id)),
            }])
            .await;
        trace!(database = ?name, "delete database");
        Ok(())
    }

    pub async fn create_collection(
        &self,
        name: String,
        database: String,
        partition: Option<co_req::Partition>,
    ) -> Result<CollectionDesc> {
        let schema = self.schema()?;
        let db = schema
            .get_database(&database)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.to_owned()))?;

        let collection = schema
            .create_collection(CollectionDesc {
                name: name.to_owned(),
                db: db.id,
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
        trace!(database = ?database, collection = ?collection, collection_id = collection.id, "create collection");

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

        let request_shard_cnt = partitions.len();
        let candidate_groups = match self.alloc.place_group_for_shard(request_shard_cnt).await {
            Ok(candidates) => {
                if candidates.is_empty() {
                    error!(
                        database = collection.db,
                        collection = ?collection.name,
                        "no avaliable group to alloc new shard, requested: {request_shard_cnt}",
                    );
                    return Err(Error::NoAvaliableGroup);
                }
                candidates
            }
            Err(err) => return Err(err),
        };

        let mut group_shards: HashMap<u64, Vec<ShardDesc>> = HashMap::new();
        for (group_idx, partition) in partitions.into_iter().enumerate() {
            let id = schema.next_shard_id().await?;
            let shard = ShardDesc {
                id,
                collection_id: collection.id.to_owned(),
                partition: Some(partition),
            };
            let group = candidate_groups
                .get(group_idx % candidate_groups.len())
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

        self.scheduler
            .setup_task(ReconcileTask {
                task: Some(Task::CreateCollectionShards(CreateCollectionShards {
                    wait_create: group_shards
                        .into_iter()
                        .map(|(group, shards)| GroupShards { group, shards })
                        .collect::<Vec<_>>(),
                    wait_cleanup: vec![],
                    step: CreateCollectionShardStep::CollectionCreating as i32,
                })),
            })
            .await;

        Ok(())
    }

    pub async fn delete_collection(&self, name: &str, database: &str) -> Result<()> {
        let schema = self.schema()?;
        let db = self
            .get_database(database)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.to_owned()))?;
        let collection = schema.get_collection(db.id, name).await?;
        if let Some(collection) = collection {
            let id = collection.id;
            schema.delete_collection(collection).await?;
            self.watcher_hub()
                .notify_deletes(vec![DeleteEvent {
                    event: Some(delete_event::Event::Collection(id)),
                }])
                .await;
        }
        trace!(database = database, collection = name, "delete collection");
        Ok(())
    }

    pub async fn list_database(&self) -> Result<Vec<DatabaseDesc>> {
        self.schema()?.list_database().await
    }

    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseDesc>> {
        self.schema()?.get_database(name).await
    }

    pub async fn list_collection(&self, database: &str) -> Result<Vec<CollectionDesc>> {
        let schema = self.schema()?;
        let db = schema
            .get_database(database)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.to_owned()))?;
        Ok(schema
            .list_collection()
            .await?
            .iter()
            .filter(|c| c.db == db.id)
            .cloned()
            .collect::<Vec<_>>())
    }

    pub async fn get_collection(
        &self,
        name: &str,
        database: &str,
    ) -> Result<Option<CollectionDesc>> {
        let db = self
            .get_database(database)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.to_owned()))?;
        self.schema()?.get_collection(db.id, name).await
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
    ) -> Result<(Vec<u8>, NodeDesc, RootDesc)> {
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
        let mut root = schema.get_root_desc().await?;
        root.root_nodes = {
            let mut nodes = ReplicaNodes(root.root_nodes);
            nodes.move_first(node.id);
            nodes.0
        };
        info!(node = node.id, addr = ?node.addr, "new node join cluster");
        Ok((cluster_id, node, root))
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
                info!(
                    group = desc.id,
                    desc = ?desc,
                    "update group_desc from node report"
                );
                update_events.push(UpdateEvent {
                    event: Some(update_event::Event::Group(desc)),
                })
            }
            if let Some(state) = u.replica_state {
                info!(
                    group = state.group_id,
                    replica = state.replica_id,
                    state = ?state,
                    "update replica_state from node report"
                );
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

    pub async fn alloc_replica(
        &self,
        group_id: u64,
        epoch: u64,
        requested_cnt: u64,
    ) -> Result<Vec<ReplicaDesc>> {
        let schema = self.schema()?;
        let group_desc = schema
            .get_group(group_id)
            .await?
            .ok_or(Error::GroupNotFound(group_id))?;
        if group_desc.epoch != epoch {
            return Err(Error::InvalidArgument("epoch not match".to_owned()));
        }
        let mut existing_replicas = group_desc
            .replicas
            .into_iter()
            .map(|r| r.node_id)
            .collect::<HashSet<u64>>();
        let replica_states = schema.group_replica_states(group_id).await?;
        for replica in replica_states {
            existing_replicas.insert(replica.node_id);
        }
        info!(
            group = group_id,
            "attemp allocate {requested_cnt} replicas for exist group"
        );

        let nodes = self
            .alloc
            .allocate_group_replica(
                existing_replicas.into_iter().collect(),
                requested_cnt as usize,
            )
            .await?;
        if nodes.len() != requested_cnt as usize {
            return Err(Error::ResourceExhausted("no enough nodes".to_owned()));
        }

        let mut replicas = Vec::with_capacity(nodes.len());
        for n in &nodes {
            let replica_id = schema.next_replica_id().await?;
            replicas.push(ReplicaDesc {
                id: replica_id,
                node_id: n.id,
                role: ReplicaRole::Voter.into(),
            });
        }
        info!(
            group = group_id,
            "advise allocate new group replicas in nodes: {:?}",
            replicas.iter().map(|r| r.node_id).collect::<Vec<_>>()
        );
        Ok(replicas)
    }
}

pub async fn fetch_root_replica(replica_table: &ReplicaRouteTable) -> Arc<Replica> {
    use futures::future::poll_fn;
    poll_fn(
        |ctx| match replica_table.current_root_replica(Some(ctx.waker().clone())) {
            Some(root_replica) => Poll::Ready(root_replica),
            None => Poll::Pending,
        },
    )
    .await
}

#[cfg(test)]
mod root_test {
    use engula_api::{
        server::v1::{
            watch_response::{update_event, UpdateEvent},
            GroupDesc,
        },
        v1::DatabaseDesc,
    };
    use futures::StreamExt;
    use tempdir::TempDir;

    use super::Config;
    use crate::{
        bootstrap::{bootstrap_cluster, INITIAL_EPOCH, ROOT_GROUP_ID},
        node::Node,
        root::Root,
        runtime::{Executor, ExecutorOwner},
        serverpb::v1::NodeIdent,
    };

    fn create_root_and_node(
        config: &Config,
        executor: Executor,
        node_ident: &NodeIdent,
    ) -> (Root, Node) {
        use crate::bootstrap::build_provider;

        let provider =
            executor.block_on(async { build_provider(config, executor.clone()).await.unwrap() });
        let root = Root::new(provider.clone(), node_ident, config.clone());
        let node = Node::new(config.clone(), provider).unwrap();
        (root, node)
    }

    #[test]
    fn boostrap_root() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let tmp_dir = TempDir::new("bootstrap_root").unwrap();
        let config = Config {
            root_dir: tmp_dir.path().to_owned(),
            ..Default::default()
        };

        let ident = NodeIdent {
            cluster_id: vec![],
            node_id: 1,
        };

        let (root, node) = create_root_and_node(&config, executor.to_owned(), &ident);
        executor.block_on(async {
            bootstrap_cluster(&node, "0.0.0.0:8888").await.unwrap();
            node.bootstrap(&ident).await.unwrap();
            root.bootstrap(&node).await.unwrap();
            // TODO: test on leader logic later.
        });
    }

    #[test]
    fn bootstrap_pending_root_replica() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let tmp_dir = TempDir::new("bootstrap_pending_root").unwrap();
        let config = Config {
            root_dir: tmp_dir.path().to_owned(),
            ..Default::default()
        };

        let ident = NodeIdent {
            cluster_id: vec![],
            node_id: 1,
        };

        let (root, node) = create_root_and_node(&config, executor.to_owned(), &ident);
        executor.block_on(async {
            node.bootstrap(&ident).await.unwrap();
            node.create_replica(
                3,
                GroupDesc {
                    id: ROOT_GROUP_ID,
                    epoch: INITIAL_EPOCH,
                    shards: vec![],
                    replicas: vec![],
                },
            )
            .await
            .unwrap();
            root.bootstrap(&node).await.unwrap();
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

        let tmp_dir = TempDir::new("watch_hub").unwrap();
        let config = Config {
            root_dir: tmp_dir.path().to_owned(),
            ..Default::default()
        };
        let (root, _node) = create_root_and_node(&config, executor.to_owned(), &ident);
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

pub mod diagnosis {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub struct Metadata {
        pub databases: Vec<Database>,
        pub nodes: Vec<Node>,
        pub groups: Vec<Group>,
        pub balanced: bool,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Database {
        pub id: u64,
        pub name: String,
        pub collections: Vec<Collection>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Collection {
        pub id: u64,
        pub mode: String,
        pub name: String,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Node {
        pub addr: String,
        pub id: u64,
        pub replicas: Vec<NodeReplica>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct NodeReplica {
        pub group: u64,
        pub id: u64,
        pub raft_role: i32,
        pub replica_role: i32,
    }

    #[derive(Serialize, Deserialize)]
    pub struct Group {
        pub epoch: u64,
        pub id: u64,
        pub replicas: Vec<GroupReplica>,
        pub shards: Vec<GroupShard>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct GroupReplica {
        pub id: u64,
        pub node: u64,
        pub raft_role: i32,
        pub replica_role: i32,
        pub term: u64,
    }

    #[derive(Serialize, Deserialize)]
    pub struct GroupShard {
        pub collection: u64,
        pub id: u64,
        pub partition: String,
    }
}
