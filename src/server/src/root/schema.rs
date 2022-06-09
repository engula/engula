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

use engula_api::{
    server::v1::{
        shard_desc::{Partition, RangePartition},
        BatchWriteRequest, GroupDesc, NodeDesc, ReplicaDesc, ReplicaRole, ShardDesc,
    },
    v1::{CollectionDesc, DatabaseDesc, PutRequest},
};
use prost::Message;

use super::store::RootStore;
use crate::{
    bootstrap::{
        FIRST_NODE_ID, FIRST_REPLICA_ID, MAX_KEY, MIN_KEY, NA_SHARD_ID, ROOT_GROUP_ID,
        ROOT_SHARD_ID,
    },
    node::group_engine::LOCAL_COLLECTION_ID,
    Error, Result,
};

const SYSTEM_DATABASE_NAME: &str = "__system__";
const SYSTEM_DATABASE_ID: u64 = 1;
const SYSTEM_COLLECTION_COLLECTION: &str = "collection";
const SYSTEM_COLLECTION_COLLECTION_ID: u64 = LOCAL_COLLECTION_ID + 1;
const SYSTEM_DATABASE_COLLECTION: &str = "database";
const SYSTEM_DATABASE_COLLECTION_ID: u64 = SYSTEM_COLLECTION_COLLECTION_ID + 1;
const SYSTEM_MATE_COLLECTION: &str = "meta";
const SYSTEM_MATE_COLLECTION_ID: u64 = SYSTEM_DATABASE_COLLECTION_ID + 1;
const SYSTEM_NODE_COLLECTION: &str = "node";
const SYSTEM_NODE_COLLECTION_ID: u64 = SYSTEM_MATE_COLLECTION_ID + 1;
const SYSTEM_GROUP_COLLECTION: &str = "group";
const SYSTEM_GROUP_COLLECTION_ID: u64 = SYSTEM_NODE_COLLECTION_ID + 1;

const META_CLUSTER_ID_KEY: &str = "cluster_id";
const META_COLLECTION_ID_KEY: &str = "collection_id";
const META_DATABASE_ID_KEY: &str = "database_id";
const META_GROUP_ID_KEY: &str = "group_id";
const META_NODE_ID_KEY: &str = "node_id";
const META_REPLICA_ID_KEY: &str = "replica_id";
const META_SHARD_ID_KEY: &str = "shard_id";

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
        self.get_database_internal(name).await
    }

    pub async fn update_database(&self, _desc: DatabaseDesc) -> Result<()> {
        todo!()
    }

    pub async fn delete_database(&self, name: &str) -> Result<()> {
        let db = self.get_database(name).await?;
        if db.is_none() {
            return Err(Error::DatabaseNotFound(name.to_owned()));
        }
        self.delete(SYSTEM_DATABASE_COLLECTION_ID, &db.unwrap().id.to_le_bytes())
            .await
    }

    pub async fn create_collection(&self, desc: CollectionDesc) -> Result<CollectionDesc> {
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_COLLECTION_ID_KEY).await?;
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
        database: &str,
        collection: &str,
    ) -> Result<Option<CollectionDesc>> {
        let db = self.get_database(database).await?;
        if db.is_none() {
            return Ok(None);
        }
        let database_id = db.unwrap().id;
        self.get_collection_internal(database_id, collection).await
    }

    pub async fn update_collection(&self, _desc: CollectionDesc) -> Result<()> {
        todo!()
    }

    pub async fn delete_collection(&self, id: u64) -> Result<()> {
        self.delete(SYSTEM_COLLECTION_COLLECTION_ID, &id.to_le_bytes())
            .await
    }

    pub async fn add_node(&self, desc: NodeDesc) -> Result<NodeDesc> {
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_NODE_ID_KEY).await?;
        Ok(desc)
    }

    pub async fn get_node(&self, id: u64) -> Result<Option<NodeDesc>> {
        self.get_node_internal(id).await
    }

    pub async fn delete_node(&self, id: u64) -> Result<()> {
        self.delete(SYSTEM_NODE_COLLECTION_ID, &id.to_le_bytes())
            .await
    }
}

// bootstrap schema.
impl Schema {
    pub async fn try_bootstrap(&mut self, addr: &str, cluster_id: Vec<u8>) -> Result<()> {
        if let Some(exist_cluster_id) = self.cluster_id().await? {
            if exist_cluster_id != cluster_id {
                return Err(Error::ClusterNotMatch);
            }
            return Ok(());
        }

        let mut batch = PutBatchBuilder::default();

        let next_collection_id = Self::init_system_collections(&mut batch);

        Self::init_meta_collection(&mut batch, next_collection_id, cluster_id);

        batch.put_database(DatabaseDesc {
            id: SYSTEM_DATABASE_ID.to_owned(),
            name: SYSTEM_DATABASE_NAME.to_owned(),
        });

        batch.put_node(NodeDesc {
            id: FIRST_NODE_ID,
            addr: addr.into(),
        });

        batch.put_group(GroupDesc {
            id: ROOT_GROUP_ID,
            replicas: vec![ReplicaDesc {
                id: FIRST_REPLICA_ID,
                node_id: FIRST_NODE_ID,
                role: ReplicaRole::Voter.into(),
            }],
            shards: vec![ShardDesc {
                id: ROOT_SHARD_ID,
                parent_id: NA_SHARD_ID,
                partition: Some(Partition::Range(RangePartition {
                    start: MIN_KEY.to_owned(),
                    end: MAX_KEY.to_owned(),
                })),
            }],
        });

        self.batch_write(batch.build()).await?;

        Ok(())
    }

    fn init_system_collections(batch: &mut PutBatchBuilder) -> u64 {
        let self_collection = CollectionDesc {
            id: SYSTEM_COLLECTION_COLLECTION_ID,
            name: SYSTEM_COLLECTION_COLLECTION.to_owned(),
            parent_id: SYSTEM_DATABASE_ID,
        };
        batch.put_collection(self_collection);

        let db_collection = CollectionDesc {
            id: SYSTEM_DATABASE_COLLECTION_ID,
            name: SYSTEM_DATABASE_COLLECTION.to_owned(),
            parent_id: SYSTEM_DATABASE_ID,
        };
        batch.put_collection(db_collection);

        let meta_collection = CollectionDesc {
            id: SYSTEM_MATE_COLLECTION_ID,
            name: SYSTEM_MATE_COLLECTION.to_owned(),
            parent_id: SYSTEM_DATABASE_ID,
        };
        batch.put_collection(meta_collection);

        let node_collection = CollectionDesc {
            id: SYSTEM_NODE_COLLECTION_ID,
            name: SYSTEM_NODE_COLLECTION.to_owned(),
            parent_id: SYSTEM_DATABASE_ID,
        };
        batch.put_collection(node_collection);

        let group_collection = CollectionDesc {
            id: SYSTEM_GROUP_COLLECTION_ID,
            name: SYSTEM_GROUP_COLLECTION.to_owned(),
            parent_id: SYSTEM_DATABASE_ID,
        };
        batch.put_collection(group_collection.to_owned());
        group_collection.id + 1
    }

    fn init_meta_collection(
        batch: &mut PutBatchBuilder,
        next_collection_id: u64,
        cluster_id: Vec<u8>,
    ) {
        batch.put_meta(META_CLUSTER_ID_KEY.into(), cluster_id);
        batch.put_meta(
            META_DATABASE_ID_KEY.into(),
            (SYSTEM_DATABASE_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_COLLECTION_ID_KEY.into(),
            next_collection_id.to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_GROUP_ID_KEY.into(),
            (ROOT_GROUP_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_NODE_ID_KEY.into(),
            (FIRST_NODE_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_REPLICA_ID_KEY.into(),
            (FIRST_REPLICA_ID + 1).to_le_bytes().to_vec(),
        );
        batch.put_meta(
            META_SHARD_ID_KEY.into(),
            (ROOT_SHARD_ID + 1).to_le_bytes().to_vec(),
        );
    }
}

// internal methods.
impl Schema {
    async fn get_database_internal(&self, name: &str) -> Result<Option<DatabaseDesc>> {
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

    async fn get_collection_internal(
        &self,
        database_id: u64,
        name: &str,
    ) -> Result<Option<CollectionDesc>> {
        let val = self
            .get(
                SYSTEM_COLLECTION_COLLECTION_ID,
                &collection_key(database_id, name),
            )
            .await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = CollectionDesc::decode(&*val.unwrap()).map_err(|_| {
            Error::InvalidData(format!("collection desc: {}, {}", database_id, name))
        })?;
        Ok(Some(desc))
    }

    async fn get_node_internal(&self, id: u64) -> Result<Option<NodeDesc>> {
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

    async fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(SYSTEM_MATE_COLLECTION_ID, key).await
    }

    async fn batch_write(&self, batch: BatchWriteRequest) -> Result<()> {
        self.store.batch_write(batch).await
    }

    async fn get(&self, collection_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.get(&data_key(collection_id, key)).await
    }

    async fn delete(&self, collection_id: u64, key: &[u8]) -> Result<()> {
        self.store.delete(&data_key(collection_id, key)).await
    }

    async fn next_id(&self, id_type: &str) -> Result<u64> {
        // TODO(zojw): replace with INC.
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

#[derive(Default)]
struct PutBatchBuilder {
    batch: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PutBatchBuilder {
    fn put(&mut self, collection_id: u64, key: Vec<u8>, val: Vec<u8>) {
        self.batch.push((data_key(collection_id, &key), val));
    }

    fn build(&self) -> BatchWriteRequest {
        let puts = self
            .batch
            .iter()
            .cloned()
            .map(|(key, value)| PutRequest { key, value })
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
            collection_key(desc.parent_id, &desc.name),
            desc.encode_to_vec(),
        );
        self
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
fn data_key(collection_id: u64, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + key.len());
    buf.extend_from_slice(collection_id.to_le_bytes().as_slice());
    buf.extend_from_slice(key);
    buf
}
