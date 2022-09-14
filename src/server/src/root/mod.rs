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
mod bg_job;
mod collector;
mod heartbeat;
mod liveness;
mod metrics;
mod schedule;
mod schema;
mod store;
mod watch;

use std::{collections::*, sync::*, task::Poll, time::Duration};

use engula_api::{
    server::v1::{report_request::GroupUpdates, watch_response::*, *},
    v1::{
        collection_desc as co_desc, create_collection_request as co_req, CollectionDesc,
        DatabaseDesc,
    },
};
use engula_client::NodeClient;
use tokio::time::Instant;
use tokio_util::time::delay_queue;
use tracing::{error, info, trace, warn};

pub(crate) use self::schema::*;
pub use self::{
    allocator::RootConfig,
    collector::RootCollector,
    watch::{WatchHub, Watcher, WatcherInitializer},
};
use self::{
    allocator::{LeaderBalancer, ReplicaBalancer, ShardBalancer, SysAllocSource},
    bg_job::Jobs,
    diagnosis::Metadata,
    schedule::ReconcileScheduler,
    schema::ReplicaNodes,
    store::RootStore,
};
use crate::{
    bootstrap::{ROOT_GROUP_ID, SHARD_MAX, SHARD_MIN},
    node::{Node, Replica, ReplicaRouteTable},
    runtime::{self, TaskPriority},
    serverpb::v1::{background_job::Job, reconcile_task, *},
    Config, Error, Provider, Result,
};

#[derive(Clone)]
pub struct Root {
    cfg: RootConfig,
    shared: Arc<RootShared>,
    alloc: Arc<allocator::Allocator<SysAllocSource>>,
    liveness: Arc<liveness::Liveness>,
    scheduler: Arc<ReconcileScheduler>,
    heartbeat_queue: Arc<HeartbeatQueue>,
    ongoing_stats: Arc<OngoingStats>,
    jobs: Arc<Jobs>,
}

pub struct RootShared {
    provider: Arc<Provider>,
    node_ident: NodeIdent,
    local_addr: String,
    cfg_cpu_nums: u32,
    core: Mutex<Option<RootCore>>,
    watcher_hub: Arc<WatchHub>,
}

impl RootShared {
    pub fn schema(&self) -> Result<Arc<Schema>> {
        let core = self.core.lock().unwrap();
        core.as_ref()
            .map(|c| c.schema.clone())
            .ok_or_else(|| Error::NotRootLeader(RootDesc::default(), 0, None))
    }
}

struct RootCore {
    schema: Arc<Schema>,
}

impl Root {
    pub(crate) fn new(provider: Arc<Provider>, node_ident: &NodeIdent, cfg: Config) -> Self {
        let local_addr = cfg.addr.clone();
        let cfg_cpu_nums = cfg.cpu_nums;
        let ongoing_stats = Arc::new(OngoingStats::default());
        let shared = Arc::new(RootShared {
            provider,
            local_addr,
            cfg_cpu_nums,
            core: Mutex::new(None),
            node_ident: node_ident.to_owned(),
            watcher_hub: Default::default(),
        });
        let liveness = Arc::new(liveness::Liveness::new(Duration::from_secs(
            cfg.root.liveness_threshold_sec,
        )));
        let heartbeat_queue = Arc::new(HeartbeatQueue::default());
        let info = Arc::new(SysAllocSource::new(shared.clone(), liveness.to_owned()));
        let replica_balancer = Arc::new(ReplicaBalancer::new(
            shared.clone(),
            info.to_owned(),
            cfg.root.to_owned(),
            heartbeat_queue.to_owned(),
        ));
        let leader_balancer = Arc::new(LeaderBalancer::with(
            info.to_owned(),
            shared.clone(),
            cfg.root.to_owned(),
            heartbeat_queue.clone(),
        ));
        let shard_balancer = Arc::new(ShardBalancer::with(
            shared.clone(),
            info.to_owned(),
            cfg.root.to_owned(),
        ));
        let alloc = Arc::new(allocator::Allocator::new(info, cfg.root.to_owned()));
        let jobs = Arc::new(Jobs::new(
            shared.to_owned(),
            alloc.to_owned(),
            heartbeat_queue.to_owned(),
            shard_balancer.to_owned(),
        ));
        let sched_ctx = schedule::ScheduleContext {
            shared: shared.clone(),
            alloc: alloc.clone(),
            leader_balancer,
            replica_balancer,
            shard_balancer,
            heartbeat_queue: heartbeat_queue.clone(),
            jobs: jobs.to_owned(),
            cfg: cfg.root.to_owned(),
        };
        let scheduler = Arc::new(schedule::ReconcileScheduler::new(sched_ctx));
        Self {
            cfg: cfg.root,
            alloc,
            shared,
            liveness,
            scheduler,
            heartbeat_queue,
            ongoing_stats,
            jobs,
        }
    }

    pub fn is_root(&self) -> bool {
        self.shared.core.lock().unwrap().is_some()
    }

    pub fn current_node_id(&self) -> u64 {
        self.shared.node_ident.node_id
    }

    pub async fn bootstrap(&self, node: &Node) -> Result<Vec<NodeDesc>> {
        let root = self.clone();
        self.shared
            .provider
            .executor
            .spawn(None, TaskPriority::Middle, async move {
                root.run_heartbeat().await;
            });
        let root = self.clone();
        self.shared
            .provider
            .executor
            .spawn(None, TaskPriority::Low, async move {
                root.run_background_jobs().await;
            });
        let replica_table = node.replica_table().clone();
        let root = self.clone();
        self.shared
            .provider
            .executor
            .spawn(None, TaskPriority::Middle, async move {
                root.run_schedule(replica_table).await;
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

    // A Daemon task to:
    // - check root leadership
    // - schedule group/replica/shard
    // - schedule heartbeat sending
    async fn run_schedule(&self, replica_table: ReplicaRouteTable) -> ! {
        let mut bootstrapped = false;
        loop {
            let root_replica = fetch_root_replica(&replica_table).await;

            // Wait the current root replica becomes a leader.
            if let Ok(Some(_)) = root_replica.on_leader("root", false).await {
                match self
                    .step_leader(
                        &self.shared.local_addr,
                        self.shared.cfg_cpu_nums,
                        root_replica,
                        &mut bootstrapped,
                    )
                    .await
                {
                    Ok(()) | Err(Error::NotLeader(..)) => {
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

    // A Deamon task to finsh handle task scheduled in heartbeat_queue and reschedule for next
    // heartbeat.
    async fn run_heartbeat(&self) -> ! {
        loop {
            if let Ok(schema) = self.schema() {
                let _timer = metrics::HEARTBEAT_STEP_DURATION_SECONDS.start_timer();
                let nodes = self.heartbeat_queue.try_poll().await;
                if !nodes.is_empty() {
                    metrics::HEARTBEAT_TASK_QUEUE_SIZE.set(nodes.len() as i64);
                    if let Err(err) = self.send_heartbeat(schema.to_owned(), &nodes).await {
                        warn!(err = ?err, "send heartbeat meet error");
                    }
                }
            }
            runtime::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn run_background_jobs(&self) -> ! {
        loop {
            if self.schema().is_ok() {
                if let Err(err) = self.jobs.advance_jobs().await {
                    warn!(err=?err, "run background job meet err");
                    runtime::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
                self.jobs.wait_more_jobs().await;
            } else {
                runtime::time::sleep(Duration::from_secs(20)).await;
            };
        }
    }

    async fn step_leader(
        &self,
        local_addr: &str,
        cfg_cpu_nums: u32,
        root_replica: Arc<Replica>,
        bootstrapped: &mut bool,
    ) -> Result<()> {
        let store = Arc::new(RootStore::new(root_replica.to_owned()));
        let mut schema = Schema::new(store.clone());

        // Only when the program is initialized is it checked for bootstrap, after which the
        // leadership change does not need to check for whether bootstrap or not.
        if !*bootstrapped {
            if let Err(err) = schema
                .try_bootstrap_root(
                    local_addr,
                    cfg_cpu_nums,
                    self.shared.node_ident.cluster_id.clone(),
                )
                .await
            {
                metrics::BOOTSTRAP_FAIL_TOTAL.inc();
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
        self::metrics::LEADER_STATE_INFO.set(1);

        self.ongoing_stats.reset();
        self.heartbeat_queue.enable(true).await;
        self.jobs.on_step_leader().await?;

        let node_id = self.shared.node_ident.node_id;
        info!(
            "node {node_id} step root service leader, heartbeat_interval: {:?}, liveness_threshold: {:?}",
            self.cfg.heartbeat_interval(),
            Duration::from_secs(self.cfg.liveness_threshold_sec),
        );

        // try schedule a full cluster heartbeat when current node become new root leader.
        let nodes = schema.list_node().await?;
        self.heartbeat_queue
            .try_schedule(
                nodes
                    .iter()
                    .map(|n| HeartbeatTask { node_id: n.id })
                    .collect::<Vec<_>>(),
                Instant::now(),
            )
            .await;

        while let Ok(Some(_)) = root_replica.to_owned().on_leader("root", true).await {
            let next_interval = self.scheduler.step_one().await;
            crate::runtime::time::sleep(next_interval).await;
            if !next_interval.is_zero() {
                self.scheduler.wait_heartbeat_tick().await;
            }
        }
        info!("node {node_id} current root node drop leader");

        // After that, RootCore needs to be set to None before returning.
        self.heartbeat_queue.enable(false).await;
        self.jobs.on_drop_leader();
        self.ongoing_stats.reset();
        {
            self.liveness.reset();

            let mut core = self.shared.core.lock().unwrap();
            *core = None;
        }

        self::metrics::LEADER_STATE_INFO.set(0);

        Ok(())
    }

    pub async fn cordon_node(&self, node_id: u64) -> Result<()> {
        let schema = self.schema()?;
        let mut node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();
        if !matches!(current_status, NodeStatus::Active) {
            return Err(crate::Error::InvalidArgument(
                "node already cordoned".into(),
            ));
        }
        node_desc.status = NodeStatus::Cordoned as i32;
        schema.update_node(node_desc).await?; // TODO: cas
        Ok(())
    }

    pub async fn uncordon_node(&self, node_id: u64) -> Result<()> {
        let schema = self.schema()?;
        let mut node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();
        if !matches!(
            current_status,
            NodeStatus::Cordoned | NodeStatus::Drained | NodeStatus::Decommissioned
        ) {
            return Err(crate::Error::InvalidArgument(
                "node status unsupport uncordon".into(),
            ));
        }

        node_desc.status = NodeStatus::Active as i32;
        schema.update_node(node_desc).await?; // TODO: cas
        Ok(())
    }

    pub async fn begin_drain(&self, node_id: u64) -> Result<()> {
        let schema = self.schema()?;

        if self.current_node_id() == node_id {
            info!("try to drain root leader and move root leadership out first");
            self.scheduler
                .setup_task(ReconcileTask {
                    task: Some(reconcile_task::Task::ShedRoot(ShedRootLeaderTask {
                        node_id,
                    })),
                })
                .await;
            return Err(crate::Error::InvalidArgument(
                "node is root leader, try again later".into(),
            ));
        }

        let mut node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();
        if !matches!(current_status, NodeStatus::Cordoned) {
            return Err(crate::Error::InvalidArgument(
                "only in cordoned status node can be drain".into(),
            ));
        }

        node_desc.status = NodeStatus::Draining as i32;
        schema.update_node(node_desc).await?; // TODO: cas

        self.scheduler
            .setup_task(ReconcileTask {
                task: Some(reconcile_task::Task::ShedLeader(ShedLeaderTask { node_id })),
            })
            .await;

        Ok(())
    }

    pub async fn node_status(&self, node_id: u64) -> Result<NodeStatus> {
        let schema = self.schema()?;
        let node_desc = schema
            .get_node(node_id)
            .await?
            .ok_or_else(|| crate::Error::InvalidArgument("node not found".into()))?;

        let current_status = NodeStatus::from_i32(node_desc.status).unwrap();

        Ok(current_status)
    }

    pub async fn nodes(&self) -> Option<u64> {
        if let Ok(schema) = self.shared.schema() {
            if let Ok(nodes) = schema.list_node().await {
                return Some(nodes.len() as u64);
            }
        }
        None
    }

    pub async fn job_state(&self) -> Result<String> {
        use serde_json::json;
        fn to_json(j: &BackgroundJob) -> serde_json::Value {
            match j.job.as_ref().unwrap() {
                Job::CreateCollection(c) => {
                    let state = format!(
                        "{:?}",
                        CreateCollectionJobStatus::from_i32(c.status).unwrap()
                    );
                    let wait_create = c.wait_create.len();
                    let wait_cleanup = c.wait_cleanup.len();
                    json!({
                        "type": "create collection",
                        "name": c.collection_name,
                        "status": state,
                        "wait_create": wait_create,
                        "wait_cleanup": wait_cleanup,
                    })
                }
                Job::CreateOneGroup(c) => {
                    let status = format!("{:?}", CreateOneGroupStatus::from_i32(c.status).unwrap());
                    let wait_create = c.wait_create.len();
                    let wait_cleanup = c.wait_cleanup.len();
                    let retired = c.create_retry;
                    let group_id = c.group_desc.as_ref().map(|g| g.id).unwrap_or_default();
                    json!({
                        "type": "create group",
                        "status": status,
                        "replica_count": c.request_replica_cnt,
                        "wait_create": wait_create,
                        "wait_cleanup": wait_cleanup,
                        "retry_count": retired,
                        "group_id": group_id,
                    })
                }
                Job::PurgeCollection(p) => {
                    json!({
                        "type": "purge collection",
                        "database": p.database_id,
                        "collection": p.collection_id,
                        "name": p.collection_name,
                    })
                }
                Job::PurgeDatabase(p) => {
                    json!({
                        "type": "purge database",
                        "database": p.database_id,
                    })
                }
            }
        }

        let schema = self.schema()?;
        let ongoing_jobs = schema.list_job().await?;
        let history_jobs = schema.list_history_job().await?;
        let ongoing = ongoing_jobs.iter().map(to_json).collect::<Vec<_>>();
        let history = history_jobs.iter().map(to_json).collect::<Vec<_>>();
        Ok(json!({"ongoing": ongoing, "history": history}).to_string())
    }

    pub async fn info(&self) -> Result<Metadata> {
        let schema = self.schema()?;
        let nodes = schema.list_node().await?;
        let groups = schema.list_group().await?;
        let replicas = groups
            .iter()
            .filter(|g| g.id != ROOT_GROUP_ID)
            .flat_map(|g| g.replicas.iter().map(|r| (r, g.id)).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let states = schema.list_replica_state().await?;
        let dbs = schema.list_database().await?;
        let collections = schema.list_collection().await?;

        let balanced = !self.scheduler.need_reconcile().await?;

        use diagnosis::*;

        Ok(Metadata {
            nodes: nodes
                .iter()
                .map(|n| {
                    let replicas = replicas
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
                        .collect::<Vec<_>>();
                    let leaders = replicas
                        .iter()
                        .cloned()
                        .filter(|r| r.raft_role == RaftRole::Leader as i32)
                        .collect::<Vec<_>>();
                    Node {
                        id: n.id,
                        addr: n.addr.to_owned(),
                        replicas,
                        leaders,
                        status: n.status,
                    }
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
        })
    }

    async fn get_node_client(&self, addr: String) -> Result<NodeClient> {
        let client = self.shared.provider.conn_manager.get_node_client(addr)?;
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
        let db = self.get_database(name).await?;
        if db.is_none() {
            return Err(Error::DatabaseNotFound(name.to_owned()));
        }
        let db = db.unwrap();
        if db.id == SYSTEM_DATABASE_ID {
            return Err(Error::InvalidArgument(
                "unsupport delete system database".into(),
            ));
        }
        self.jobs
            .submit(
                BackgroundJob {
                    job: Some(Job::PurgeDatabase(PurgeDatabaseJob {
                        database_id: db.id,
                        database_name: db.name.to_owned(),
                        created_time: format!("{:?}", Instant::now()),
                    })),
                    ..Default::default()
                },
                false,
            )
            .await?;
        let schema = self.schema()?;
        let id = schema.delete_database(&db).await?;
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
            .prepare_create_collection(CollectionDesc {
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
        trace!(database = ?database, collection = ?collection, collection_id = collection.id, "prepare create collection");

        self.do_create_collection(schema.to_owned(), collection.to_owned())
            .await?;

        self.watcher_hub()
            .notify_updates(vec![UpdateEvent {
                event: Some(update_event::Event::Collection(collection.to_owned())),
            }])
            .await;

        Ok(collection)
    }

    async fn do_create_collection(
        &self,
        schema: Arc<Schema>,
        collection: CollectionDesc,
    ) -> Result<()> {
        let wait_create = {
            let partition = collection
                .partition
                .as_ref()
                .unwrap_or(&co_desc::Partition::Hash(co_desc::HashPartition {
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

            let mut wait_create = Vec::new();
            for partition in partitions {
                let id = schema.next_shard_id().await?;
                let shard = ShardDesc {
                    id,
                    collection_id: collection.id.to_owned(),
                    partition: Some(partition),
                };
                wait_create.push(shard);
            }
            wait_create
        };

        self.jobs
            .submit(
                BackgroundJob {
                    job: Some(Job::CreateCollection(CreateCollectionJob {
                        database: collection.db,
                        collection_name: collection.name.to_owned(),
                        wait_create,
                        status: CreateCollectionJobStatus::CreateCollectionCreating as i32,
                        desc: Some(collection.to_owned()),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                true,
            )
            .await?;

        Ok(())
    }

    pub async fn delete_collection(&self, name: &str, database: &DatabaseDesc) -> Result<()> {
        let schema = self.schema()?;
        let db = self
            .get_database(&database.name)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.name.clone()))?;
        let collection = schema.get_collection(db.id, name).await?;
        if let Some(collection) = collection {
            if collection.id < USER_COLLECTION_INIT_ID {
                return Err(Error::InvalidArgument(
                    "unsupported delete system collection".into(),
                ));
            }
            let collection_id = collection.id;
            let database_name = db.name.to_owned();
            let collection_name = collection.name.to_owned();
            self.jobs
                .submit(
                    BackgroundJob {
                        job: Some(Job::PurgeCollection(PurgeCollectionJob {
                            database_id: db.id,
                            collection_id,
                            database_name,
                            collection_name,
                            created_time: format!("{:?}", Instant::now()),
                        })),
                        ..Default::default()
                    },
                    false,
                )
                .await?;
            schema.delete_collection(collection).await?;
            self.watcher_hub()
                .notify_deletes(vec![DeleteEvent {
                    event: Some(delete_event::Event::Collection(collection_id)),
                }])
                .await;
        }
        trace!(
            collection = name,
            "delete collection, database {}",
            database.name
        );
        Ok(())
    }

    pub async fn list_database(&self) -> Result<Vec<DatabaseDesc>> {
        self.schema()?.list_database().await
    }

    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseDesc>> {
        self.schema()?.get_database(name).await
    }

    pub async fn list_collection(&self, database: &DatabaseDesc) -> Result<Vec<CollectionDesc>> {
        let schema = self.schema()?;
        let db = schema
            .get_database(&database.name)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.name.clone()))?;
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
        database: &DatabaseDesc,
    ) -> Result<Option<CollectionDesc>> {
        let db = self
            .get_database(&database.name)
            .await?
            .ok_or_else(|| Error::DatabaseNotFound(database.name.clone()))?;
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
        self.heartbeat_queue
            .try_schedule(vec![HeartbeatTask { node_id: node.id }], Instant::now())
            .await;
        info!(node = node.id, addr = ?node.addr, "new node join cluster");
        Ok((cluster_id, node, root))
    }

    pub async fn report(&self, updates: Vec<GroupUpdates>) -> Result<()> {
        // mock report doesn't work.
        // return Ok(());

        let ongoing_stats = self.ongoing_stats.clone();
        let schema = self.schema()?;
        let mut update_events = Vec::new();
        let mut changed_group_states = Vec::new();
        for u in updates {
            let group_desc = if let Some(update_group) = &u.group_desc {
                match schema.get_group(u.group_id).await? {
                    Some(pre_group) if pre_group.epoch >= update_group.epoch => None,
                    _ => u.group_desc,
                }
            } else {
                None
            };

            let replica_state = if let Some(update_replica_state) = &u.replica_state {
                match schema
                    .get_replica_state(u.group_id, update_replica_state.replica_id)
                    .await?
                {
                    Some(pre_rs)
                        if pre_rs.term > update_replica_state.term
                            || (pre_rs.term == update_replica_state.term
                                && pre_rs.role == update_replica_state.role) =>
                    {
                        None
                    }
                    _ => u.replica_state,
                }
            } else {
                None
            };
            schema
                .update_group_replica(group_desc.to_owned(), replica_state.to_owned())
                .await?;

            if let Some(sched_state) = u.schedule_state {
                ongoing_stats.handle_update(&[sched_state], None);
            }

            if let Some(desc) = group_desc {
                info!(
                    group = desc.id,
                    desc = ?desc,
                    "update group_desc from node report"
                );
                if desc.id == ROOT_GROUP_ID {
                    self.heartbeat_queue
                        .try_schedule(
                            vec![HeartbeatTask {
                                node_id: self.current_node_id(),
                            }],
                            Instant::now(),
                        )
                        .await;
                }
                metrics::ROOT_UPDATE_GROUP_DESC_TOTAL.report.inc();
                update_events.push(UpdateEvent {
                    event: Some(update_event::Event::Group(desc)),
                })
            }
            if let Some(state) = replica_state {
                info!(
                    group = state.group_id,
                    replica = state.replica_id,
                    state = ?state,
                    "update replica_state from node report"
                );
                metrics::ROOT_UPDATE_REPLICA_STATE_TOTAL.report.inc();
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
            "attempt allocate {requested_cnt} replicas for exist group"
        );

        let nodes = self
            .alloc
            .allocate_group_replica(
                existing_replicas.into_iter().collect(),
                requested_cnt as usize,
            )
            .await?;
        if nodes.len() != requested_cnt as usize {
            warn!("non enough nodes to allocate replicas, exist nodes: {}, requested: {requested_cnt}", nodes.len());
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

#[derive(Debug)]
pub enum QueueTask {
    Heartbeat(HeartbeatTask),
    Sentinel(Sentinel),
}

#[derive(Debug)]
pub struct HeartbeatTask {
    pub node_id: u64,
}

#[derive(Debug)]
pub struct Sentinel {
    sender: futures::channel::oneshot::Sender<()>,
}

#[derive(Default)]
pub struct HeartbeatQueue {
    core: Arc<futures::lock::Mutex<HeartbeatQueueCore>>,
}

#[derive(Default)]
struct HeartbeatQueueCore {
    enable: bool,
    delay: delay_queue::DelayQueue<QueueTask>,
    node_scheduled: HashMap<u64, (delay_queue::Key, Instant)>,
}

impl HeartbeatQueue {
    pub async fn try_schedule(&self, tasks: Vec<HeartbeatTask>, when: Instant) {
        let mut core = self.core.lock().await;
        if !core.enable {
            return;
        }
        for task in tasks {
            let node = task.node_id;
            if let Some((scheduled_key, old_when)) =
                core.node_scheduled.get(&node).map(ToOwned::to_owned)
            {
                if when < old_when {
                    metrics::HEARTBEAT_RESCHEDULE_EARLY_INTERVAL_SECONDS
                        .observe(old_when.saturating_duration_since(when).as_secs_f64());
                    core.delay.reset_at(&scheduled_key, when);
                    core.node_scheduled.insert(node, (scheduled_key, when));
                    trace!(node=node, when=?when, "update next heartbeat");
                }
            } else {
                let key = core.delay.insert_at(QueueTask::Heartbeat(task), when);
                core.node_scheduled.insert(node, (key, when));
                trace!(node=node, when=?when, "schedule next heartbeat");
            }
        }
    }

    pub async fn wait_one_heartbeat_tick(&self) {
        let (sender, receiver) = futures::channel::oneshot::channel::<()>();
        let sentinel = Sentinel { sender };
        {
            let mut core = self.core.lock().await;
            if !core.enable {
                return;
            }
            core.delay
                .insert(QueueTask::Sentinel(sentinel), Duration::from_millis(0));
        }
        let _ = receiver.await;
    }

    async fn try_poll(&self) -> Vec<HeartbeatTask> {
        let mut core = self.core.lock().await;
        if !core.enable {
            return vec![];
        }
        let tasks = futures::future::poll_fn(|cx| {
            let mut tasks = Vec::new();
            while let Poll::Ready(Some(task)) = core.delay.poll_expired(cx) {
                tasks.push(task);
            }
            Poll::Ready(tasks)
        })
        .await;
        let tasks = tasks
            .into_iter()
            .map(|e| e.into_inner())
            .collect::<Vec<_>>();
        let mut heartbeats = Vec::new();
        for task in tasks {
            match task {
                QueueTask::Heartbeat(task) => {
                    core.node_scheduled.remove(&task.node_id);
                    heartbeats.push(task);
                }
                QueueTask::Sentinel(sential) => {
                    let _ = sential.sender.send(());
                }
            }
        }
        heartbeats
    }

    async fn enable(&self, enable: bool) {
        let mut core = self.core.lock().await;
        if core.enable != enable {
            core.node_scheduled.clear();
            core.delay.clear();
            core.enable = enable;
        }
    }
}

struct GroupDelta {
    epoch: u64,
    incoming: Vec<ReplicaDesc>,
    outgoing: Vec<ReplicaDesc>,
}

#[derive(Clone, Default)]
pub struct NodeDelta {
    pub replica_count: i64,
    // TODO: qps
}

#[derive(Default, Clone)]
pub struct OngoingStats {
    sched_stats: Arc<Mutex<SchedStats>>,
    job_stats: Arc<Mutex<JobStats>>,
}

#[derive(Default)]
struct SchedStats {
    raw_group_delta: HashMap<u64 /* group */, GroupDelta>,
    node_view: HashMap<u64 /* node */, NodeDelta>,
}

#[derive(Default)]
struct JobStats {
    node_delta: HashMap<u64 /* node_id */, NodeDelta>,
}

impl OngoingStats {
    fn handle_update(
        &self,
        state_updates: &[ScheduleState],
        job_updates: Option<HashMap<u64 /* node */, NodeDelta>>,
    ) {
        if !state_updates.is_empty() {
            let mut inner = self.sched_stats.lock().unwrap();
            if inner.replace_state(state_updates) {
                inner.rebuild_view();
            }
        }
        if job_updates.is_some() {
            let mut inner = self.job_stats.lock().unwrap();
            inner.node_delta = job_updates.as_ref().unwrap().to_owned();
        }
    }

    #[allow(dead_code)]
    pub fn get_node_delta(&self, node: u64) -> NodeDelta {
        let mut rs = NodeDelta::default();
        if let Some(sched_node_delta) = {
            let inner = self.sched_stats.lock().unwrap();
            inner.node_view.get(&node).map(ToOwned::to_owned)
        } {
            rs.replica_count += sched_node_delta.replica_count;
        }
        if let Some(job_node_delta) = {
            let inner = self.job_stats.lock().unwrap();
            inner.node_delta.get(&node).map(ToOwned::to_owned)
        } {
            rs.replica_count += job_node_delta.replica_count;
        }
        rs
    }

    pub fn reset(&self) {
        {
            let mut inner = self.sched_stats.lock().unwrap();
            inner.raw_group_delta.clear();
            inner.node_view.clear();
        }
        {
            let mut inner = self.job_stats.lock().unwrap();
            inner.node_delta.clear();
        }
    }
}

impl SchedStats {
    fn replace_state(&mut self, updates: &[ScheduleState]) -> bool {
        let mut updated = false;
        for state in updates {
            match self.raw_group_delta.entry(state.group_id) {
                hash_map::Entry::Occupied(mut ent) => {
                    let delta = ent.get_mut();
                    if delta.epoch < state.epoch {
                        *delta = GroupDelta {
                            epoch: state.epoch,
                            incoming: state.incoming_replicas.to_owned(),
                            outgoing: state.outgoing_replicas.to_owned(),
                        };
                        updated = true;
                    }
                }
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(GroupDelta {
                        epoch: state.epoch,
                        incoming: state.incoming_replicas.to_owned(),
                        outgoing: state.outgoing_replicas.to_owned(),
                    });
                    updated = true;
                }
            }
        }
        updated
    }

    fn rebuild_view(&mut self) {
        let mut new_node_view: HashMap<u64, NodeDelta> = HashMap::new();
        for r in self.raw_group_delta.values() {
            for incoming in &r.incoming {
                match new_node_view.entry(incoming.node_id) {
                    hash_map::Entry::Occupied(mut ent) => ent.get_mut().replica_count += 1,
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(NodeDelta { replica_count: 1 });
                    }
                }
            }
            for outgoing in &r.outgoing {
                match new_node_view.entry(outgoing.node_id) {
                    hash_map::Entry::Occupied(mut ent) => ent.get_mut().replica_count -= 1,
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(NodeDelta { replica_count: -1 });
                    }
                }
            }
        }
        self.node_view = new_node_view;
    }
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
        pub leaders: Vec<NodeReplica>,
        pub status: i32,
    }

    #[derive(Serialize, Deserialize, Clone)]
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
