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

use std::{collections::HashMap, sync::Arc};

use engula_api::{
    server::v1::*,
    v1::{create_collection_request::*, *},
};
use tokio::sync::Mutex;

use crate::{
    AdminRequestBuilder, AdminResponseExtractor, NodeClient, RequestBatchBuilder, RetryState,
    RootClient, Router,
};

#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Mutex<ClientInner>>,
}

#[derive(Debug, Clone)]
struct ClientInner {
    root_client: RootClient,
    router: Router,
    node_clients: HashMap<u64, NodeClient>,
}

impl Client {
    pub async fn connect(addr: String) -> Result<Self, crate::Error> {
        let root_client = RootClient::connect(vec![addr.clone()]).await?;
        let router = Router::new(vec![addr]).await;
        Ok(Self {
            inner: Arc::new(Mutex::new(ClientInner {
                root_client,
                router,
                node_clients: HashMap::new(),
            })),
        })
    }

    pub async fn create_database(&self, name: String) -> Result<Database, crate::Error> {
        let inner = self.inner.lock().await;
        let root_client = inner.root_client.clone();
        let resp = root_client
            .admin(AdminRequestBuilder::create_database(name.clone()))
            .await?;
        match AdminResponseExtractor::create_database(resp) {
            None => Err(crate::Error::NotFound(format!("database {}", name))),
            Some(desc) => Ok(Database {
                desc,
                client: self.clone(),
            }),
        }
    }

    pub async fn open_database(&self, name: String) -> Result<Database, crate::Error> {
        let inner = self.inner.lock().await;
        let root_client = inner.root_client.clone();
        let resp = root_client
            .admin(AdminRequestBuilder::get_database(name.clone()))
            .await?;
        match AdminResponseExtractor::get_database(resp) {
            None => Err(crate::Error::NotFound(format!("database {}", name))),
            Some(desc) => Ok(Database {
                desc,
                client: self.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    desc: DatabaseDesc,
    client: Client,
}

pub enum Partition {
    Hash { slots: u32 },
    Range,
}

impl From<Partition> for create_collection_request::Partition {
    fn from(p: Partition) -> Self {
        match p {
            Partition::Hash { slots } => {
                create_collection_request::Partition::Hash(HashPartition { slots })
            }
            Partition::Range => create_collection_request::Partition::Range(RangePartition {}),
        }
    }
}

impl Database {
    pub async fn create_collection(
        &self,
        name: String,
        partition: Option<Partition>,
    ) -> Result<Collection, crate::Error> {
        let client = self.client.clone();
        let db_desc = self.desc.clone();
        let inner = client.inner.lock().await;
        let root_client = inner.root_client.clone();
        let resp = root_client
            .admin(AdminRequestBuilder::create_collection(
                db_desc.name.clone(),
                name.clone(),
                partition.map(Into::into),
            ))
            .await?;
        match AdminResponseExtractor::create_collection(resp) {
            None => Err(crate::Error::NotFound(format!("collection {}", name))),
            Some(co_desc) => Ok(Collection {
                db_desc,
                co_desc,
                client: client.clone(),
            }),
        }
    }

    pub async fn open_collection(&self, name: String) -> Result<Collection, crate::Error> {
        let client = self.client.clone();
        let db_desc = self.desc.clone();
        let inner = client.inner.lock().await;
        let root_client = inner.root_client.clone();
        let resp = root_client
            .admin(AdminRequestBuilder::get_collection(
                db_desc.name.clone(),
                name.clone(),
            ))
            .await?;
        match AdminResponseExtractor::get_collection(resp) {
            None => Err(crate::Error::NotFound(format!("collection {}", name))),
            Some(co_desc) => Ok(Collection {
                db_desc,
                co_desc,
                client: client.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Collection {
    #[allow(unused)]
    db_desc: DatabaseDesc,
    co_desc: CollectionDesc,
    client: Client,
}

impl Collection {
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), crate::Error> {
        let mut retry_state = RetryState::default();

        loop {
            match self.put_inner(&key, &value).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if !retry_state.retry(&err).await {
                        return Err(err);
                    }
                }
            }
        }
    }

    pub async fn put_inner(&self, key: &[u8], value: &[u8]) -> Result<(), crate::Error> {
        let mut inner = self.client.inner.lock().await;
        let router = inner.router.clone();
        let shard = router.find_shard(self.co_desc.clone(), key)?;
        let group = router.find_group_by_shard(shard.id)?;
        let epoch = group
            .epoch
            .ok_or_else(|| crate::Error::NotFound(format!("epoch (key={:?})", key)))?;
        let leader_id = group
            .leader_id
            .ok_or_else(|| crate::Error::NotFound(format!("leader_id (key={:?})", key)))?;
        let node_id = group
            .replicas
            .get(&leader_id)
            .map(|desc| desc.node_id)
            .ok_or_else(|| crate::Error::NotFound(format!("leader_node_id (key={:?})", key)))?;
        let client = inner.node_clients.get(&node_id);
        let resp = match client {
            None => {
                let addr = router.find_node_addr(node_id)?;
                let client = NodeClient::connect(addr).await?;
                inner.node_clients.insert(node_id, client.clone());
                client
                    .batch_group_requests(
                        RequestBatchBuilder::new(node_id)
                            .put(group.id, epoch, shard.id, key.to_vec(), value.to_vec())
                            .build(),
                    )
                    .await?
            }
            Some(client) => {
                client
                    .batch_group_requests(
                        RequestBatchBuilder::new(node_id)
                            .put(group.id, epoch, shard.id, key.to_vec(), value.to_vec())
                            .build(),
                    )
                    .await?
            }
        };

        for r in resp {
            if let Some(err) = crate::Error::from_group_response(&r) {
                return Err(err);
            }
        }

        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, crate::Error> {
        let mut retry_state = RetryState::default();

        loop {
            match self.get_inner(&key).await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !retry_state.retry(&err).await {
                        return Err(err);
                    }
                }
            }
        }
    }

    pub async fn get_inner(&self, key: &[u8]) -> Result<Option<Vec<u8>>, crate::Error> {
        let mut inner = self.client.inner.lock().await;
        let router = inner.router.clone();
        let shard = router.find_shard(self.co_desc.clone(), key)?;
        let group = router.find_group_by_shard(shard.id)?;
        let epoch = group
            .epoch
            .ok_or_else(|| crate::Error::NotFound(format!("epoch (key={:?})", key)))?;
        let leader = group
            .leader_id
            .ok_or_else(|| crate::Error::NotFound(format!("leader unavailable (key={:?})", key)))?;
        let node_id = group
            .replicas
            .get(&leader)
            .map(|desc| desc.node_id)
            .ok_or_else(|| crate::Error::NotFound(format!("node_id (key={:?})", key)))?;
        let client = inner.node_clients.get(&node_id);
        let resp = match client {
            None => {
                let addr = router.find_node_addr(node_id)?;
                let client = NodeClient::connect(addr).await?;
                inner.node_clients.insert(node_id, client.clone());
                client
                    .batch_group_requests(
                        RequestBatchBuilder::new(node_id)
                            .get(group.id, epoch, shard.id, key.to_vec())
                            .build(),
                    )
                    .await?
            }
            Some(client) => {
                client
                    .batch_group_requests(
                        RequestBatchBuilder::new(node_id)
                            .get(group.id, epoch, shard.id, key.to_vec())
                            .build(),
                    )
                    .await?
            }
        };

        for r in resp {
            if let Some(err) = crate::Error::from_group_response(&r) {
                return Err(err);
            }
            if let Some(GroupResponseUnion {
                response: Some(group_response_union::Response::Get(GetResponse { value })),
            }) = r.response
            {
                return Ok(value);
            }
        }

        Ok(None)
    }
}
