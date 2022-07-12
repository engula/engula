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
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use engula_api::{server::v1::*, v1::*};
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{info, warn};

use crate::RootClient;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Router {
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Clone, Default)]
pub struct State {
    node_id_lookup: HashMap<u64, String /* ip:port */>,
    db_id_lookup: HashMap<u64, DatabaseDesc>,
    db_name_lookup: HashMap<String, u64>,
    co_id_lookup: HashMap<u64, CollectionDesc>,
    co_name_lookup: HashMap<(u64 /* db */, String), u64>,
    co_shards_lookup: HashMap<u64 /* co */, Vec<ShardDesc>>,
    shard_group_lookup: HashMap<u64 /* shard */, u64 /* group */>,
    group_id_lookup: HashMap<u64 /* group */, RouterGroupState>,
}

#[allow(unused)]
#[derive(Debug, Clone, Default)]
pub struct RouterGroupState {
    pub id: u64,
    pub epoch: Option<u64>,
    pub leader_id: Option<u64>,
    pub replicas: HashMap<u64, ReplicaDesc>,
}

#[allow(unused)]
impl Router {
    pub async fn new(addrs: Vec<String>) -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let state_clone = state.clone();
        tokio::spawn(async move {
            state_main(state_clone, addrs).await;
        });
        Self { state }
    }

    pub fn find_shard(&self, desc: CollectionDesc, key: &[u8]) -> Result<ShardDesc, crate::Error> {
        if let Some(collection_desc::Partition::Hash(collection_desc::HashPartition { slots })) =
            desc.partition
        {
            // TODO: it's temp hash impl..
            let crc = crc32fast::hash(key);
            let slot = crc % (slots as u32);

            let state = self.state.lock().unwrap();

            let shards = state
                .co_shards_lookup
                .get(&desc.id)
                .ok_or_else(|| crate::Error::NotFound(format!("shard (key={:?})", key)))?;

            if slots != shards.len() as u32 {
                return Err(crate::Error::NotFound("expired shard info".into()));
            }

            let shard = shards
                .iter()
                .find(|s| {
                    if let shard_desc::Partition::Hash(p) = s.partition.as_ref().unwrap() {
                        if p.slot_id == slot {
                            return true;
                        }
                    }
                    false
                })
                .unwrap();

            return Ok(shard.clone());
        }

        let state = self.state.lock().unwrap();
        let shards = state
            .co_shards_lookup
            .get(&desc.id)
            .ok_or_else(|| crate::Error::NotFound(format!("shard (key={:?})", key)))?;
        for shard in shards {
            if let Some(shard_desc::Partition::Range(shard_desc::RangePartition { start, end })) =
                shard.partition.clone()
            {
                if start.as_slice() > key {
                    continue;
                }
                if (end.as_slice() < key) || (end.is_empty())
                /* end = vec![] means MAX */
                {
                    return Ok(shard.clone());
                }
            }
        }
        Err(crate::Error::NotFound(format!("shard (key={:?})", key)))
    }

    pub fn find_group_by_shard(&self, shard: u64) -> Result<RouterGroupState, crate::Error> {
        let state = self.state.lock().unwrap();
        let group = state
            .shard_group_lookup
            .get(&shard)
            .and_then(|id| state.group_id_lookup.get(id))
            .cloned();
        group.ok_or_else(|| crate::Error::NotFound(format!("group (shard={:?})", shard)))
    }

    pub fn find_group(&self, id: u64) -> Result<RouterGroupState, crate::Error> {
        let state = self.state.lock().unwrap();
        let group = state.group_id_lookup.get(&id).cloned();
        group.ok_or_else(|| crate::Error::NotFound(format!("group (id={:?})", id)))
    }

    pub fn find_node_addr(&self, id: u64) -> Result<String, crate::Error> {
        let state = self.state.lock().unwrap();
        let addr = state.node_id_lookup.get(&id).cloned();
        addr.ok_or_else(|| crate::Error::NotFound(format!("node_addr (node_id={:?})", id)))
    }

    pub fn total_nodes(&self) -> usize {
        self.state.lock().unwrap().node_id_lookup.len()
    }
}

async fn state_main(state: Arc<Mutex<State>>, addrs: Vec<String>) {
    let mut interval = 1;
    let root_client = loop {
        match RootClient::connect(addrs.clone()).await {
            Ok(c) => break c,
            Err(e) => {
                warn!(err = ?e, addr=?addrs, "connect root server");
            }
        };

        tokio::time::sleep(Duration::from_millis(interval)).await;
        interval = std::cmp::min(interval * 2, 1000);
    };

    info!("start watching events...");

    interval = 1;
    loop {
        let cur_group_epochs = {
            let state = state.lock().unwrap();
            state
                .group_id_lookup
                .iter()
                .map(|(id, s)| (*id, s.epoch.unwrap_or_default()))
                .collect()
        };
        let events = match root_client.watch(cur_group_epochs).await {
            Ok(events) => events,
            Err(e) => {
                warn!(err = ?e, "watch events");
                tokio::time::sleep(Duration::from_millis(interval)).await;
                interval = std::cmp::min(interval * 2, 1000);
                continue;
            }
        };

        interval = 1;
        watch_events(state.as_ref(), events).await;
    }
}

async fn watch_events(state: &Mutex<State>, mut events: Streaming<WatchResponse>) {
    use watch_response::{delete_event::Event as DeleteEvent, update_event::Event as UpdateEvent};

    while let Some(event) = events.next().await {
        let (updates, deletes) = match event {
            Ok(resp) => (resp.updates, resp.deletes),
            Err(status) => {
                warn!("WatchEvent error: {}", status);
                continue;
            }
        };
        for update in updates {
            let event = match update.event {
                None => continue,
                Some(event) => event,
            };
            let mut state = state.lock().unwrap();
            match event {
                UpdateEvent::Node(node_desc) => {
                    state.node_id_lookup.insert(node_desc.id, node_desc.addr);
                }
                UpdateEvent::Group(group_desc) => {
                    let (id, epoch) = (group_desc.id, group_desc.epoch);
                    let (shards, replicas) = (group_desc.shards, group_desc.replicas);

                    let replicas = replicas
                        .into_iter()
                        .map(|d| (d.id, d))
                        .collect::<HashMap<u64, ReplicaDesc>>();
                    let mut group_state = RouterGroupState {
                        id,
                        epoch: Some(epoch),
                        leader_id: None,
                        replicas,
                    };
                    if let Some(old_state) = state.group_id_lookup.get(&id) {
                        group_state.leader_id = old_state.leader_id;
                    }
                    state.group_id_lookup.insert(id, group_state);

                    for shard in shards {
                        state.shard_group_lookup.insert(shard.id, id);

                        let co_shards_lookup = &mut state.co_shards_lookup;
                        match co_shards_lookup.get_mut(&shard.collection_id) {
                            None => {
                                co_shards_lookup.insert(shard.collection_id, vec![shard]);
                            }
                            Some(shards) => {
                                shards.retain(|s| s.id != shard.id);
                                shards.push(shard);
                            }
                        }
                    }
                }
                UpdateEvent::GroupState(group_state) => {
                    let id = group_state.group_id;
                    let leader_id = group_state.leader_id;
                    let mut group_state = RouterGroupState {
                        id,
                        epoch: None,
                        leader_id,
                        replicas: HashMap::new(),
                    };
                    if let Some(old_state) = state.group_id_lookup.get(&id) {
                        group_state.epoch = old_state.epoch;
                        group_state.replicas = old_state.replicas.clone();
                    }
                    state.group_id_lookup.insert(id, group_state);
                }
                UpdateEvent::Database(db_desc) => {
                    let desc = db_desc.clone();
                    let (id, name) = (db_desc.id, db_desc.name);
                    if let Some(old_desc) = state.db_id_lookup.insert(id, desc) {
                        if old_desc.name != name {
                            state.db_name_lookup.remove(&name);
                        }
                    }
                    state.db_name_lookup.insert(name, id);
                }
                UpdateEvent::Collection(co_desc) => {
                    let desc = co_desc.clone();
                    let (id, name, db) = (co_desc.id, co_desc.name, co_desc.db);
                    if let Some(old_desc) = state.co_id_lookup.insert(id, desc) {
                        if old_desc.name != name {
                            state.co_name_lookup.remove(&(db, old_desc.name));
                        }
                    }
                    state.co_name_lookup.insert((db, name), id);
                }
            }
        }
        for delete in deletes {
            let event = match delete.event {
                None => continue,
                Some(event) => event,
            };
            let mut state = state.lock().unwrap();
            match event {
                DeleteEvent::Node(node) => {
                    state.node_id_lookup.remove(&node);
                }
                DeleteEvent::Group(_) => todo!(),
                DeleteEvent::GroupState(_) => todo!(),
                DeleteEvent::Database(db) => {
                    if let Some(desc) = state.db_id_lookup.remove(&db) {
                        state.db_name_lookup.remove(desc.name.as_str());
                    }
                }
                DeleteEvent::Collection(co) => {
                    if let Some(desc) = state.co_id_lookup.remove(&co) {
                        state.co_name_lookup.remove(&(desc.db, desc.name));
                    }
                }
            }
        }
    }
}
