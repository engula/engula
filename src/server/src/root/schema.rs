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
    collections::{hash_map::Entry, BTreeMap, HashMap},
    sync::Arc,
};

use engula_api::{
    server::v1::{
        group_request_union::Request,
        group_response_union::Response,
        shard_desc::{Partition, RangePartition},
        watch_response::{delete_event, update_event, DeleteEvent, UpdateEvent},
        *,
    },
    v1::{collection_desc, CollectionDesc, DatabaseDesc, DeleteRequest, PutRequest},
};
use engula_client::GroupClient;
use futures::lock::Mutex;
use prost::Message;
use tracing::{info, warn};

use super::store::RootStore;
use crate::{
    bootstrap::*,
    node::{
        engine::{SnapshotMode, LOCAL_COLLECTION_ID},
        GroupEngine,
    },
    serverpb::v1::BackgroundJob,
    Error, Provider, Result,
};

const SYSTEM_DATABASE_NAME: &str = "__system__";
pub const SYSTEM_DATABASE_ID: u64 = 1;
const SYSTEM_COLLECTION_COLLECTION: &str = "collection";
const SYSTEM_COLLECTION_COLLECTION_ID: u64 = LOCAL_COLLECTION_ID + 1;
const SYSTEM_COLLECTION_COLLECTION_SHARD: u64 = 1;
const SYSTEM_DATABASE_COLLECTION: &str = "database";
const SYSTEM_DATABASE_COLLECTION_ID: u64 = SYSTEM_COLLECTION_COLLECTION_ID + 1;
const SYSTEM_DATABASE_COLLECTION_SHARD: u64 = SYSTEM_COLLECTION_COLLECTION_SHARD + 1;
const SYSTEM_MATE_COLLECTION: &str = "meta";
const SYSTEM_MATE_COLLECTION_ID: u64 = SYSTEM_DATABASE_COLLECTION_ID + 1;
const SYSTEM_MATE_COLLECTION_SHARD: u64 = SYSTEM_DATABASE_COLLECTION_SHARD + 1;
const SYSTEM_NODE_COLLECTION: &str = "node";
const SYSTEM_NODE_COLLECTION_ID: u64 = SYSTEM_MATE_COLLECTION_ID + 1;
const SYSTEM_NODE_COLLECTION_SHARD: u64 = SYSTEM_MATE_COLLECTION_SHARD + 1;
const SYSTEM_GROUP_COLLECTION: &str = "group";
const SYSTEM_GROUP_COLLECTION_ID: u64 = SYSTEM_NODE_COLLECTION_ID + 1;
const SYSTEM_GROUP_COLLECTION_SHARD: u64 = SYSTEM_NODE_COLLECTION_SHARD + 1;
const SYSTEM_REPLICA_STATE_COLLECTION: &str = "replica_state";
const SYSTEM_REPLICA_STATE_COLLECTION_ID: u64 = SYSTEM_GROUP_COLLECTION_ID + 1;
const SYSTEM_REPLICA_STATE_COLLECTION_SHARD: u64 = SYSTEM_GROUP_COLLECTION_SHARD + 1;
const SYSTEM_JOB_COLLECTION: &str = "job";
const SYSTEM_JOB_COLLECTION_ID: u64 = SYSTEM_REPLICA_STATE_COLLECTION_ID + 1;
const SYSTEM_JOB_COLLECTION_SHARD: u64 = SYSTEM_REPLICA_STATE_COLLECTION_SHARD + 1;
const SYSTEM_JOB_HISTORY_COLLECTION: &str = "job_history";
const SYSTEM_JOB_HISTORY_COLLECTION_ID: u64 = SYSTEM_JOB_COLLECTION_ID + 1;
const SYSTEM_JOB_HISTORY_COLLECTION_SHARD: u64 = SYSTEM_JOB_COLLECTION_SHARD + 1;

pub const USER_COLLECTION_INIT_ID: u64 = SYSTEM_JOB_HISTORY_COLLECTION_ID + 1;

const META_CLUSTER_ID_KEY: &str = "cluster_id";
const META_COLLECTION_ID_KEY: &str = "collection_id";
const META_DATABASE_ID_KEY: &str = "database_id";
const META_GROUP_ID_KEY: &str = "group_id";
const META_NODE_ID_KEY: &str = "node_id";
const META_REPLICA_ID_KEY: &str = "replica_id";
const META_SHARD_ID_KEY: &str = "shard_id";
const META_JOB_ID_KEY: &str = "job_id";

lazy_static::lazy_static! {
    pub static ref SYSTEM_COLLECTION_SHARD: BTreeMap<u64, u64> = BTreeMap::from([
        (SYSTEM_COLLECTION_COLLECTION_ID, SYSTEM_COLLECTION_COLLECTION_SHARD),
        (SYSTEM_DATABASE_COLLECTION_ID, SYSTEM_DATABASE_COLLECTION_SHARD),
        (SYSTEM_MATE_COLLECTION_ID, SYSTEM_MATE_COLLECTION_SHARD),
        (SYSTEM_NODE_COLLECTION_ID, SYSTEM_NODE_COLLECTION_SHARD),
        (SYSTEM_GROUP_COLLECTION_ID, SYSTEM_GROUP_COLLECTION_SHARD),
        (SYSTEM_REPLICA_STATE_COLLECTION_ID, SYSTEM_REPLICA_STATE_COLLECTION_SHARD),
        (SYSTEM_JOB_COLLECTION_ID, SYSTEM_JOB_COLLECTION_SHARD),
        (SYSTEM_JOB_HISTORY_COLLECTION_ID, SYSTEM_JOB_HISTORY_COLLECTION_SHARD),
    ]);
    pub static ref ID_GEN_LOCKS: HashMap<String, Mutex<()>> = HashMap::from([
        (META_CLUSTER_ID_KEY.to_owned(), Mutex::new(())),
        (META_COLLECTION_ID_KEY.to_owned(),  Mutex::new(())),
        (META_DATABASE_ID_KEY.to_owned(),  Mutex::new(())),
        (META_GROUP_ID_KEY.to_owned(),  Mutex::new(())),
        (META_NODE_ID_KEY.to_owned(),  Mutex::new(())),
        (META_REPLICA_ID_KEY.to_owned(),  Mutex::new(())),
        (META_SHARD_ID_KEY.to_owned(),  Mutex::new(())),
        (META_JOB_ID_KEY.to_owned(), Mutex::new(())),
    ]);
}

#[derive(Clone)]
pub struct Schema {
    store: Arc<RootStore>,
}

// public interface.
impl Schema {
    pub fn new(store: Arc<RootStore>) -> Self {
        Self { store }
    }

    pub async fn cluster_id(&self) -> Result<Option<Vec<u8>>> {
        let id = self.get_meta(META_CLUSTER_ID_KEY.as_bytes()).await?;
        if let Some(id) = id {
            return Ok(Some(id));
        }
        Ok(None)
    }

    pub async fn create_database(&self, desc: DatabaseDesc) -> Result<DatabaseDesc> {
        if self.get_database(&desc.name).await?.is_some() {
            return Err(Error::AlreadyExists(format!(
                "database {}",
                desc.name.to_owned()
            )));
        }

        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_DATABASE_ID_KEY).await?;
        self.batch_write(
            PutBatchBuilder::default()
                .put_database(desc.to_owned())
                .build(),
        )
        .await?;
        Ok(desc)
    }

    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseDesc>> {
        let val = self
            .get(SYSTEM_DATABASE_COLLECTION_ID, name.as_bytes())
            .await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = DatabaseDesc::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData(format!("database desc: {}", name)))?;
        Ok(Some(desc))
    }

    pub async fn update_database(&self, _desc: DatabaseDesc) -> Result<()> {
        todo!()
    }

    pub async fn delete_database(&self, db: &DatabaseDesc) -> Result<u64> {
        self.delete(SYSTEM_DATABASE_COLLECTION_ID, db.name.as_bytes())
            .await?;
        Ok(db.id)
    }

    pub async fn list_database(&self) -> Result<Vec<DatabaseDesc>> {
        let vals = self.list(SYSTEM_DATABASE_COLLECTION_ID).await?;
        let mut databases = Vec::new();
        for val in vals {
            databases.push(
                DatabaseDesc::decode(&*val)
                    .map_err(|_| Error::InvalidData("database desc".into()))?,
            );
        }
        Ok(databases)
    }

    pub async fn prepare_create_collection(&self, desc: CollectionDesc) -> Result<CollectionDesc> {
        if self.get_collection(desc.db, &desc.name).await?.is_some() {
            return Err(Error::AlreadyExists(format!(
                "collection {}",
                desc.name.to_owned()
            )));
        }
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_COLLECTION_ID_KEY).await?;
        Ok(desc)
    }

    pub async fn create_collection(&self, desc: CollectionDesc) -> Result<CollectionDesc> {
        assert!(self.get_collection(desc.db, &desc.name).await?.is_none());
        self.batch_write(
            PutBatchBuilder::default()
                .put_collection(desc.to_owned())
                .build(),
        )
        .await?;
        Ok(desc)
    }

    pub async fn get_collection(
        &self,
        database: u64,
        collection: &str,
    ) -> Result<Option<CollectionDesc>> {
        let val = self
            .get(
                SYSTEM_COLLECTION_COLLECTION_ID,
                &collection_key(database, collection),
            )
            .await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = CollectionDesc::decode(&*val.unwrap()).map_err(|_| {
            Error::InvalidData(format!("collection desc: {}, {}", database, collection))
        })?;
        Ok(Some(desc))
    }

    pub async fn get_collection_shards(&self, collection_id: u64) -> Result<Vec<(u64, ShardDesc)>> {
        let groups = self.list_group().await?;
        let group_shards = groups
            .iter()
            .flat_map(|g| {
                g.shards
                    .iter()
                    .filter(|s| s.collection_id == collection_id)
                    .map(|s| (g.id, s.to_owned()))
            })
            .collect::<Vec<_>>();
        Ok(group_shards)
    }

    pub async fn update_collection(&self, _desc: CollectionDesc) -> Result<()> {
        todo!()
    }

    pub async fn delete_collection(&self, collection: CollectionDesc) -> Result<()> {
        self.delete(
            SYSTEM_COLLECTION_COLLECTION_ID,
            &collection_key(collection.db, &collection.name),
        )
        .await
    }

    pub async fn list_collection(&self) -> Result<Vec<CollectionDesc>> {
        let vals = self.list(SYSTEM_COLLECTION_COLLECTION_ID).await?;
        let mut collections = Vec::new();
        for val in vals {
            let c = CollectionDesc::decode(&*val)
                .map_err(|_| Error::InvalidData("collection desc".into()))?;
            collections.push(c);
        }
        Ok(collections)
    }

    pub async fn list_database_collections(&self, database: u64) -> Result<Vec<CollectionDesc>> {
        let collections = self.list_collection().await?;
        Ok(collections
            .into_iter()
            .filter(|c| c.db == database)
            .collect::<Vec<_>>())
    }

    pub async fn add_node(&self, desc: NodeDesc) -> Result<NodeDesc> {
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_NODE_ID_KEY).await?;
        self.batch_write(PutBatchBuilder::default().put_node(desc.to_owned()).build())
            .await?;
        Ok(desc)
    }

    pub async fn get_node(&self, id: u64) -> Result<Option<NodeDesc>> {
        let val = self
            .get(SYSTEM_NODE_COLLECTION_ID, &id.to_le_bytes())
            .await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = NodeDesc::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData(format!("node desc: {}", id)))?;
        Ok(Some(desc))
    }

    pub async fn delete_node(&self, id: u64) -> Result<()> {
        self.delete(SYSTEM_NODE_COLLECTION_ID, &id.to_le_bytes())
            .await
    }

    pub async fn update_node(&self, desc: NodeDesc) -> Result<()> {
        self.batch_write(PutBatchBuilder::default().put_node(desc.to_owned()).build())
            .await?;
        Ok(())
    }

    pub async fn list_node(&self) -> Result<Vec<NodeDesc>> {
        let vals = self.list(SYSTEM_NODE_COLLECTION_ID).await?;
        let mut nodes = Vec::new();
        for val in vals {
            nodes
                .push(NodeDesc::decode(&*val).map_err(|_| Error::InvalidData("node desc".into()))?);
        }
        Ok(nodes)
    }

    pub async fn list_node_raw(engine: GroupEngine) -> Result<Vec<NodeDesc>> {
        let shard_id = Self::system_shard_id(SYSTEM_NODE_COLLECTION_ID); // System collection only have one shard.
        let mut snapshot = match engine.snapshot(shard_id, SnapshotMode::Prefix { key: &[] }) {
            Ok(snapshot) => snapshot,
            Err(Error::ShardNotFound(_)) => {
                // This replica of root group haven't initialized.
                return Ok(vec![]);
            }
            Err(e) => {
                warn!("root list nodes raw: {e:?}");
                return Err(e);
            }
        };
        let mut nodes = Vec::new();
        for mvcc in snapshot.iter() {
            for entry in mvcc {
                if let Some(val) = entry.value() {
                    nodes.push(
                        NodeDesc::decode(val)
                            .map_err(|_| Error::InvalidData("node desc".into()))?,
                    );
                }
            }
        }

        Ok(nodes)
    }

    pub async fn update_group_replica(
        &self,
        group: Option<GroupDesc>,
        replica: Option<ReplicaState>,
    ) -> Result<()> {
        let mut builder = PutBatchBuilder::default();
        if group.is_some() {
            builder.put_group(group.unwrap());
        }
        if replica.is_some() {
            builder.put_replica_state(replica.unwrap());
        }
        if builder.is_empty() {
            return Ok(());
        }
        self.batch_write(builder.build()).await?;
        Ok(())
    }

    pub async fn remove_replica_state(&self, group_id: u64, replica_id: u64) -> Result<()> {
        let key = replica_key(group_id, replica_id);
        self.delete(SYSTEM_REPLICA_STATE_COLLECTION_ID, &key).await
    }

    pub async fn get_group(&self, id: u64) -> Result<Option<GroupDesc>> {
        let val = self
            .get(SYSTEM_GROUP_COLLECTION_ID, &id.to_le_bytes())
            .await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = GroupDesc::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData(format!("group desc: {}", id)))?;
        Ok(Some(desc))
    }

    pub async fn delete_group(&self, id: u64) -> Result<()> {
        // TODO: prefix delete replica_state
        self.delete(SYSTEM_GROUP_COLLECTION_ID, &id.to_le_bytes())
            .await
    }

    pub async fn list_group(&self) -> Result<Vec<GroupDesc>> {
        let vals = self.list(SYSTEM_GROUP_COLLECTION_ID).await?;
        let mut groups = Vec::new();
        for val in vals {
            groups.push(
                GroupDesc::decode(&*val).map_err(|_| Error::InvalidData("group desc".into()))?,
            );
        }
        Ok(groups)
    }

    pub async fn get_replica_state(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Result<Option<ReplicaState>> {
        let key = replica_key(group_id, replica_id);
        let val = self.get(SYSTEM_REPLICA_STATE_COLLECTION_ID, &key).await?;
        if val.is_none() {
            return Ok(None);
        }
        let state = ReplicaState::decode(&*val.unwrap()).map_err(|_| {
            Error::InvalidData(format!(
                "replica_state: group: {}, replica: {}",
                group_id, replica_id
            ))
        })?;
        Ok(Some(state))
    }

    pub async fn list_replica_state(&self) -> Result<Vec<ReplicaState>> {
        let vals = self.list(SYSTEM_REPLICA_STATE_COLLECTION_ID).await?;
        let mut states = Vec::with_capacity(vals.len());
        for val in vals {
            let state = ReplicaState::decode(&*val)
                .map_err(|_| Error::InvalidData("replica state desc".into()))?;
            states.push(state.to_owned());
        }
        Ok(states)
    }

    pub async fn group_replica_states(&self, group_id: u64) -> Result<Vec<ReplicaState>> {
        let vals = self
            .list_prefix(
                SYSTEM_REPLICA_STATE_COLLECTION_ID,
                group_id.to_le_bytes().as_slice(),
            )
            .await?;
        let mut states = Vec::with_capacity(vals.len());
        for val in vals {
            let state = ReplicaState::decode(&*val)
                .map_err(|_| Error::InvalidData("replica state desc".into()))?;
            states.push(state);
        }
        Ok(states)
    }

    pub async fn list_group_state(&self) -> Result<Vec<GroupState>> {
        let mut states: HashMap<u64, GroupState> = HashMap::new();
        for state in self.list_replica_state().await? {
            match states.entry(state.group_id) {
                Entry::Occupied(mut ent) => {
                    let group = ent.get_mut();
                    if state.role == RaftRole::Leader as i32 {
                        (*group).leader_id = Some(state.replica_id);
                    } else if (*group).leader_id == Some(state.replica_id) {
                        (*group).leader_id = None;
                    }
                    (*group)
                        .replicas
                        .retain(|desc| desc.replica_id != state.replica_id);
                    (*group).replicas.push(state);
                }
                Entry::Vacant(ent) => {
                    let leader_id = if state.role == RaftRole::Leader as i32 {
                        Some(state.replica_id)
                    } else {
                        None
                    };
                    ent.insert(GroupState {
                        group_id: state.group_id,
                        leader_id,
                        replicas: vec![state],
                    });
                }
            }
        }
        Ok(states.into_iter().map(|(_, v)| v).collect())
    }

    pub async fn get_root_desc(&self) -> Result<RootDesc> {
        let group_desc = self
            .get_group(ROOT_GROUP_ID)
            .await?
            .ok_or(Error::GroupNotFound(ROOT_GROUP_ID))?;
        let mut nodes = HashMap::new();
        for replica in &group_desc.replicas {
            let node = replica.node_id;
            if nodes.contains_key(&node) {
                continue;
            }
            let node = self
                .get_node(node)
                .await?
                .ok_or_else(|| Error::InvalidData(format!("node {} data not found", node)))?;
            nodes.insert(node.id, node);
        }
        Ok(RootDesc {
            epoch: group_desc.epoch,
            root_nodes: nodes.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
        })
    }

    pub async fn list_all_events(
        &self,
        cur_groups: HashMap<u64, u64>,
    ) -> Result<(Vec<UpdateEvent>, Vec<DeleteEvent>)> {
        let mut updates = Vec::new();
        let mut deletes = Vec::new();

        // list nodes.
        let nodes = self
            .list_node()
            .await?
            .into_iter()
            .map(|desc| UpdateEvent {
                event: Some(update_event::Event::Node(desc)),
            })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&nodes);

        // list databases.
        let dbs = self
            .list_database()
            .await?
            .into_iter()
            .map(|desc| UpdateEvent {
                event: Some(update_event::Event::Database(desc)),
            })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&dbs);

        // list collections.
        let collections = self
            .list_collection()
            .await?
            .into_iter()
            .map(|desc| UpdateEvent {
                event: Some(update_event::Event::Collection(desc)),
            })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&collections);

        // list groups.
        let groups = self
            .list_group()
            .await?
            .into_iter()
            .map(|desc| (desc.id, desc))
            .collect::<HashMap<u64, GroupDesc>>();

        let changed_groups = groups
            .iter()
            .filter(|(_, desc)| {
                if let Some(cur_epoch) = cur_groups.get(&desc.id) {
                    desc.epoch > *cur_epoch
                } else {
                    true
                }
            })
            .map(|(id, desc)| (id.to_owned(), desc.to_owned()))
            .collect::<HashMap<u64, GroupDesc>>();

        updates.extend_from_slice(
            &changed_groups
                .values()
                .into_iter()
                .map(|desc| UpdateEvent {
                    event: Some(update_event::Event::Group(desc.to_owned())),
                })
                .collect::<Vec<_>>(),
        );

        if !cur_groups.is_empty() {
            let deleted = cur_groups
                .keys()
                .into_iter()
                .filter(|group_id| !groups.contains_key(group_id))
                .collect::<Vec<_>>();
            let delete_desc = deleted
                .iter()
                .map(|id| DeleteEvent {
                    event: Some(delete_event::Event::Group(**id)),
                })
                .collect::<Vec<_>>();
            let delete_state = deleted
                .iter()
                .map(|id| DeleteEvent {
                    event: Some(delete_event::Event::GroupState(**id)),
                })
                .collect::<Vec<_>>();
            deletes.extend_from_slice(&delete_desc);
            deletes.extend_from_slice(&delete_state);
        }

        // list group_state.
        let group_states = self
            .list_group_state()
            .await?
            .into_iter()
            .filter(|desc| changed_groups.contains_key(&desc.group_id))
            .map(|desc| UpdateEvent {
                event: Some(update_event::Event::GroupState(desc)),
            })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&group_states);

        Ok((updates, deletes))
    }

    pub async fn append_job(&self, desc: BackgroundJob) -> Result<BackgroundJob> {
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_JOB_ID_KEY).await?;
        self.batch_write(PutBatchBuilder::default().put_job(desc.to_owned()).build())
            .await?;
        Ok(desc)
    }

    pub async fn remove_job(&self, job: &BackgroundJob) -> Result<()> {
        self.batch_write(
            PutBatchBuilder::default()
                .put_job_history(job.to_owned())
                .build(),
        )
        .await?;
        self.delete(SYSTEM_JOB_COLLECTION_ID, &job.id.to_le_bytes())
            .await?;
        Ok(())
    }

    pub async fn update_job(&self, desc: BackgroundJob) -> Result<bool> {
        if self
            .get(SYSTEM_JOB_COLLECTION_ID, &desc.id.to_be_bytes())
            .await?
            .is_none()
        {
            // TODO: replace this with storage put_condition operation.
            return Ok(false);
        }
        self.batch_write(PutBatchBuilder::default().put_job(desc).build())
            .await?;
        Ok(true)
    }

    pub async fn list_job(&self) -> Result<Vec<BackgroundJob>> {
        let vals = self.list(SYSTEM_JOB_COLLECTION_ID).await?;
        let mut jobs = Vec::with_capacity(vals.len());
        for val in vals {
            let job = BackgroundJob::decode(&*val)
                .map_err(|_| Error::InvalidData("backgroud job".into()))?;
            jobs.push(job.to_owned());
        }
        Ok(jobs)
    }

    pub async fn list_history_job(&self) -> Result<Vec<BackgroundJob>> {
        let vals = self.list(SYSTEM_JOB_HISTORY_COLLECTION_ID).await?;
        let mut jobs = Vec::with_capacity(vals.len());
        for val in vals {
            let job = BackgroundJob::decode(&*val)
                .map_err(|_| Error::InvalidData("backgroud job".into()))?;
            jobs.push(job.to_owned());
        }
        Ok(jobs)
    }

    pub async fn get_job_history(&self, id: &u64) -> Result<Option<BackgroundJob>> {
        let val = self
            .get(SYSTEM_JOB_HISTORY_COLLECTION_ID, &id.to_le_bytes())
            .await?;
        if val.is_none() {
            return Ok(None);
        }
        let job = BackgroundJob::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData("backgroud job".into()))?;
        Ok(Some(job))
    }
}

pub struct ReplicaNodes(pub Vec<NodeDesc>);

impl From<ReplicaNodes> for Vec<NodeDesc> {
    fn from(r: ReplicaNodes) -> Self {
        r.0
    }
}

impl ReplicaNodes {
    pub fn move_first(&mut self, id: u64) {
        if let Some(idx) = self.0.iter().position(|n| n.id == id) {
            if idx != 0 {
                self.0.swap(0, idx)
            }
        }
    }
}

// bootstrap schema.
impl Schema {
    pub async fn try_bootstrap_root(
        &mut self,
        addr: &str,
        cfg_cpu_nums: u32,
        cluster_id: Vec<u8>,
    ) -> Result<()> {
        let _timer = super::metrics::BOOTSTRAP_DURATION_SECONDS.start_timer();

        if let Some(exist_cluster_id) = self.cluster_id().await? {
            if exist_cluster_id != cluster_id {
                return Err(Error::ClusterNotMatch);
            }
            return Ok(());
        }

        info!(cluster = ?String::from_utf8_lossy(&cluster_id), "start boostrap root");

        let mut batch = PutBatchBuilder::default();

        Self::init_system_collections(&mut batch);

        let (shards, next_shard_id) = Schema::init_shards();

        Self::init_meta_collection(&mut batch, next_shard_id, cluster_id.to_owned());

        batch.put_database(DatabaseDesc {
            id: SYSTEM_DATABASE_ID.to_owned(),
            name: SYSTEM_DATABASE_NAME.to_owned(),
        });

        let cpu_nums = if cfg_cpu_nums == 0 {
            num_cpus::get() as f64
        } else {
            cfg_cpu_nums as f64
        };
        batch.put_node(NodeDesc {
            id: FIRST_NODE_ID,
            addr: addr.into(),
            capacity: Some(NodeCapacity {
                cpu_nums,
                replica_count: 1,
                leader_count: 0,
            }),
            status: NodeStatus::Active as i32,
        });

        batch.put_group(GroupDesc {
            id: ROOT_GROUP_ID,
            epoch: INITIAL_EPOCH,
            replicas: vec![ReplicaDesc {
                id: FIRST_REPLICA_ID,
                node_id: FIRST_NODE_ID,
                role: ReplicaRole::Voter.into(),
            }],
            shards,
        });

        batch.put_group(GroupDesc {
            id: INIT_USER_GROUP_ID,
            epoch: INITIAL_EPOCH,
            replicas: vec![ReplicaDesc {
                id: INIT_USER_REPLICA_ID,
                node_id: FIRST_NODE_ID,
                role: ReplicaRole::Voter.into(),
            }],
            shards: vec![],
        });

        batch.put_replica_state(ReplicaState {
            replica_id: FIRST_REPLICA_ID,
            group_id: ROOT_GROUP_ID,
            term: 0,
            voted_for: FIRST_REPLICA_ID,
            role: RaftRole::Leader.into(),
            node_id: FIRST_NODE_ID,
        });

        batch.put_replica_state(ReplicaState {
            replica_id: INIT_USER_REPLICA_ID,
            group_id: INIT_USER_GROUP_ID,
            term: 0,
            voted_for: INIT_USER_REPLICA_ID,
            role: RaftRole::Leader.into(),
            node_id: FIRST_NODE_ID,
        });

        self.batch_write(batch.build()).await?;

        info!(cluster = ?String::from_utf8_lossy(&cluster_id), "boostrap root successfully");

        Ok(())
    }

    pub fn init_shards() -> (Vec<ShardDesc>, u64) {
        let mut desc = Vec::with_capacity(SYSTEM_COLLECTION_SHARD.len());
        for (collect_id, shard_id) in SYSTEM_COLLECTION_SHARD.iter() {
            desc.push(ShardDesc {
                id: shard_id.to_owned(),
                collection_id: collect_id.to_owned(),
                partition: Some(Partition::Range(RangePartition {
                    start: SHARD_MIN.to_owned(),
                    end: SHARD_MAX.to_owned(),
                })),
            })
        }
        (desc, SYSTEM_JOB_HISTORY_COLLECTION_SHARD + 1)
    }

    pub fn system_shard_id(collection_id: u64) -> u64 {
        let shard = SYSTEM_COLLECTION_SHARD.get(&collection_id);
        if shard.is_none() {
            panic!("no such shard exists for system collection {collection_id}");
        }
        shard.unwrap().to_owned()
    }

    pub async fn next_group_id(&self) -> Result<u64> {
        self.next_id(META_GROUP_ID_KEY).await
    }

    pub async fn next_replica_id(&self) -> Result<u64> {
        self.next_id(META_REPLICA_ID_KEY).await
    }

    pub async fn next_shard_id(&self) -> Result<u64> {
        self.next_id(META_SHARD_ID_KEY).await
    }

    fn init_system_collections(batch: &mut PutBatchBuilder) {
        let self_collection = CollectionDesc {
            id: SYSTEM_COLLECTION_COLLECTION_ID,
            name: SYSTEM_COLLECTION_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(self_collection);

        let db_collection = CollectionDesc {
            id: SYSTEM_DATABASE_COLLECTION_ID,
            name: SYSTEM_DATABASE_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(db_collection);

        let meta_collection = CollectionDesc {
            id: SYSTEM_MATE_COLLECTION_ID,
            name: SYSTEM_MATE_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(meta_collection);

        let node_collection = CollectionDesc {
            id: SYSTEM_NODE_COLLECTION_ID,
            name: SYSTEM_NODE_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(node_collection);

        let group_collection = CollectionDesc {
            id: SYSTEM_GROUP_COLLECTION_ID,
            name: SYSTEM_GROUP_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(group_collection);

        let replica_state_collection = CollectionDesc {
            id: SYSTEM_REPLICA_STATE_COLLECTION_ID,
            name: SYSTEM_REPLICA_STATE_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(replica_state_collection);

        let job_collection = CollectionDesc {
            id: SYSTEM_JOB_COLLECTION_ID,
            name: SYSTEM_JOB_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(job_collection);

        let job_history_collection = CollectionDesc {
            id: SYSTEM_JOB_HISTORY_COLLECTION_ID,
            name: SYSTEM_JOB_HISTORY_COLLECTION.to_owned(),
            db: SYSTEM_DATABASE_ID,
            partition: Some(collection_desc::Partition::Range(
                collection_desc::RangePartition {},
            )),
        };
        batch.put_collection(job_history_collection);
    }

    fn init_meta_collection(batch: &mut PutBatchBuilder, next_shard_id: u64, cluster_id: Vec<u8>) {
        batch.put_meta(META_CLUSTER_ID_KEY.into(), cluster_id);
        batch.put_meta(
            META_DATABASE_ID_KEY.into(),
            (SYSTEM_DATABASE_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_COLLECTION_ID_KEY.into(),
            USER_COLLECTION_INIT_ID.to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_GROUP_ID_KEY.into(),
            (INIT_USER_GROUP_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_NODE_ID_KEY.into(),
            (FIRST_NODE_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_REPLICA_ID_KEY.into(),
            (INIT_USER_REPLICA_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_SHARD_ID_KEY.into(),
            next_shard_id.to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_JOB_ID_KEY.into(),
            INITIAL_JOB_ID.to_le_bytes().to_vec(),
        );
    }
}

// internal methods.
impl Schema {
    async fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(SYSTEM_MATE_COLLECTION_ID, key).await
    }

    async fn batch_write(&self, batch: BatchWriteRequest) -> Result<()> {
        self.store.batch_write(batch).await
    }

    async fn get(&self, collection_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let shard_id = Self::system_shard_id(collection_id);
        self.store.get(shard_id, key).await
    }

    async fn delete(&self, collection_id: u64, key: &[u8]) -> Result<()> {
        let shard_id = Self::system_shard_id(collection_id);
        self.store.delete(shard_id, key).await
    }

    async fn list(&self, collection_id: u64) -> Result<Vec<Vec<u8>>> {
        self.list_prefix(collection_id, &[]).await
    }

    async fn list_prefix(&self, collection_id: u64, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let shard_id = Self::system_shard_id(collection_id); // System collection only have one shard.
        self.store.list(shard_id, prefix).await
    }

    async fn next_id(&self, id_type: &str) -> Result<u64> {
        let _mutex = ID_GEN_LOCKS
            .get(id_type)
            .expect("id gen lock not found")
            .lock()
            .await;
        let id = self
            .get_meta(id_type.as_bytes())
            .await?
            .ok_or_else(|| Error::InvalidData(format!("{} id", id_type)))?;
        let id = u64::from_le_bytes(
            id.try_into()
                .map_err(|_| Error::InvalidData(format!("{} id", id_type)))?,
        );
        self.batch_write(
            PutBatchBuilder::default()
                .put_meta(id_type.as_bytes().to_vec(), (id + 1).to_le_bytes().to_vec())
                .build(),
        )
        .await?;
        Ok(id)
    }
}

#[derive(Clone)]
pub struct RemoteStore {
    provider: Arc<Provider>,
}

impl RemoteStore {
    pub(crate) fn new(provider: Arc<Provider>) -> Self {
        RemoteStore { provider }
    }

    pub async fn list_replica_state(&self, group_id: u64) -> Result<Vec<ReplicaState>> {
        let shard_id = Schema::system_shard_id(SYSTEM_REPLICA_STATE_COLLECTION_ID);
        let prefix = group_key(group_id);

        let req = Request::PrefixList(ShardPrefixListRequest { shard_id, prefix });
        let mut client = GroupClient::new(
            ROOT_GROUP_ID,
            self.provider.router.clone(),
            self.provider.conn_manager.clone(),
        );
        let values = match client.request(&req).await? {
            Response::PrefixList(ShardPrefixListResponse { values }) => values,
            _ => {
                return Err(Error::InvalidData(
                    "invalid response type, `SharedPrefixListResponse` is required".to_owned(),
                ))
            }
        };

        let mut states = vec![];
        for value in values {
            if let Ok(state) = ReplicaState::decode(value.as_slice()) {
                states.push(state);
            }
        }
        Ok(states)
    }

    pub async fn clear_replica_state(&self, group_id: u64, replica_id: u64) -> Result<()> {
        let shard_id = Schema::system_shard_id(SYSTEM_REPLICA_STATE_COLLECTION_ID);
        let key = replica_key(group_id, replica_id);

        let req = Request::Delete(ShardDeleteRequest {
            shard_id,
            delete: Some(DeleteRequest { key }),
        });
        let mut client = GroupClient::new(
            ROOT_GROUP_ID,
            self.provider.router.clone(),
            self.provider.conn_manager.clone(),
        );
        client.request(&req).await?;
        Ok(())
    }
}

#[derive(Default)]
struct PutBatchBuilder {
    batch: Vec<(u64, Vec<u8>, Vec<u8>)>,
}

impl PutBatchBuilder {
    fn put(&mut self, collection_id: u64, key: Vec<u8>, val: Vec<u8>) {
        let shard_id = Schema::system_shard_id(collection_id);
        self.batch.push((shard_id, key, val));
    }

    fn build(&self) -> BatchWriteRequest {
        let puts = self
            .batch
            .iter()
            .cloned()
            .map(|(shard_id, key, value)| ShardPutRequest {
                shard_id,
                put: Some(PutRequest { key, value }),
            })
            .collect::<Vec<_>>();
        BatchWriteRequest {
            puts,
            ..Default::default()
        }
    }

    fn put_meta(&mut self, key: Vec<u8>, val: Vec<u8>) -> &mut Self {
        self.put(SYSTEM_MATE_COLLECTION_ID, key, val);
        self
    }

    fn put_group(&mut self, desc: GroupDesc) -> &mut Self {
        self.put(
            SYSTEM_GROUP_COLLECTION_ID,
            desc.id.to_le_bytes().to_vec(),
            desc.encode_to_vec(),
        );
        self
    }

    fn put_replica_state(&mut self, state: ReplicaState) -> &mut Self {
        self.put(
            SYSTEM_REPLICA_STATE_COLLECTION_ID,
            replica_key(state.group_id, state.replica_id),
            state.encode_to_vec(),
        );
        self
    }

    fn put_node(&mut self, desc: NodeDesc) -> &mut Self {
        self.put(
            SYSTEM_NODE_COLLECTION_ID,
            desc.id.to_le_bytes().to_vec(),
            desc.encode_to_vec(),
        );
        self
    }

    fn put_database(&mut self, desc: DatabaseDesc) -> &mut Self {
        self.put(
            SYSTEM_DATABASE_COLLECTION_ID,
            desc.name.as_bytes().to_vec(),
            desc.encode_to_vec(),
        );
        self
    }

    fn put_collection(&mut self, desc: CollectionDesc) -> &mut Self {
        self.put(
            SYSTEM_COLLECTION_COLLECTION_ID,
            collection_key(desc.db, &desc.name),
            desc.encode_to_vec(),
        );
        self
    }

    fn put_job(&mut self, desc: BackgroundJob) -> &mut Self {
        self.put(
            SYSTEM_JOB_COLLECTION_ID,
            desc.id.to_le_bytes().to_vec(),
            desc.encode_to_vec(),
        );
        self
    }

    fn put_job_history(&mut self, desc: BackgroundJob) -> &mut Self {
        self.put(
            SYSTEM_JOB_HISTORY_COLLECTION_ID,
            desc.id.to_le_bytes().to_vec(),
            desc.encode_to_vec(),
        );
        self
    }

    fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }
}

#[inline]
fn collection_key(database_id: u64, collection_name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + collection_name.len());
    buf.extend_from_slice(database_id.to_le_bytes().as_slice());
    buf.extend_from_slice(collection_name.as_bytes());
    buf
}

#[inline]
fn group_key(group_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>());
    buf.extend_from_slice(group_id.to_le_bytes().as_slice());
    buf
}

#[inline]
fn replica_key(group_id: u64, replica_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() * 2);
    buf.extend_from_slice(group_id.to_le_bytes().as_slice());
    buf.extend_from_slice(replica_id.to_le_bytes().as_slice());
    buf
}
