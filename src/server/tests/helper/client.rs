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
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use engula_api::{server::v1::*, v1::CollectionDesc};
use engula_client::{
    ConnManager, EngulaClient, GroupClient, NodeClient, RootClient, Router, RouterGroupState,
    StaticServiceDiscovery,
};
use engula_server::{runtime, Result};

pub async fn node_client_with_retry(addr: &str) -> NodeClient {
    for _ in 0..10000 {
        match NodeClient::connect(addr.to_string()).await {
            Ok(client) => return client,
            Err(_) => {
                runtime::time::sleep(Duration::from_millis(50)).await;
            }
        };
    }
    panic!("connect to {} timeout", addr);
}

#[allow(unused)]
pub struct ClusterClient {
    nodes: HashMap<u64, String>,
    router: Router,
    conn_manager: ConnManager,
}

#[allow(unused)]
impl ClusterClient {
    pub async fn new(nodes: HashMap<u64, String>) -> Self {
        let conn_manager = ConnManager::new();
        let discovery = Arc::new(StaticServiceDiscovery::new(
            nodes.values().cloned().collect(),
        ));
        let root_client = RootClient::new(discovery, conn_manager.clone());
        let router = Router::new(root_client).await;
        ClusterClient {
            nodes,
            router,
            conn_manager,
        }
    }

    pub async fn create_replica(&self, node_id: u64, replica_id: u64, desc: GroupDesc) {
        let node_addr = self.nodes.get(&node_id).unwrap();
        let client = node_client_with_retry(node_addr).await;
        client.create_replica(replica_id, desc).await.unwrap();
    }

    pub fn group(&self, group_id: u64) -> GroupClient {
        GroupClient::new(group_id, self.router.clone(), self.conn_manager.clone())
    }

    pub async fn app_client(&self) -> EngulaClient {
        let addrs = self.nodes.values().cloned().collect::<Vec<_>>();
        EngulaClient::connect(addrs).await.unwrap()
    }

    pub async fn group_members(&self, group_id: u64) -> Vec<(u64, i32)> {
        if let Ok(state) = self.router.find_group(group_id) {
            let mut current = state
                .replicas
                .iter()
                .map(|(k, v)| (*k, v.role))
                .collect::<Vec<_>>();
            current.sort_unstable();
            current
        } else {
            vec![]
        }
    }

    pub async fn assert_group_members(&self, group_id: u64, mut replicas: Vec<u64>) {
        replicas.sort_unstable();
        for _ in 0..10000 {
            let members = self.group_members(group_id).await;
            let mut members = members
                .into_iter()
                .filter(|(_, v)| *v == ReplicaRole::Voter as i32)
                .map(|(k, _)| k)
                .collect::<Vec<u64>>();
            members.sort_unstable();
            if members == replicas {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} does not have expected replicas {replicas:?}");
    }

    pub async fn assert_num_group_voters(&self, group_id: u64, size: usize) {
        for _ in 0..10000 {
            let members = self.group_members(group_id).await;
            if members
                .into_iter()
                .filter(|(_, v)| *v == ReplicaRole::Voter as i32)
                .count()
                == size
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} does not have expected number of voters ({size})");
    }

    pub async fn assert_group_contains_member(&self, group_id: u64, replica_id: u64) {
        for _ in 0..10000 {
            if let Ok(state) = self.router.find_group(group_id) {
                if state.replicas.contains_key(&replica_id) {
                    return;
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is not contains replica {replica_id}");
    }

    pub async fn assert_group_not_contains_member(&self, group_id: u64, replica_id: u64) {
        for _ in 0..10000 {
            if let Ok(state) = self.router.find_group(group_id) {
                if !state.replicas.contains_key(&replica_id) {
                    return;
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is contains replica {replica_id}");
    }

    pub async fn assert_group_not_contains_node(&self, group_id: u64, node_id: u64) {
        for _ in 0..10000 {
            if let Ok(state) = self.router.find_group(group_id) {
                if !state.replicas.iter().any(|(_, r)| r.node_id == node_id) {
                    return;
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is contains node {node_id}");
    }

    pub async fn get_group_leader(&self, group_id: u64) -> Option<u64> {
        self.router
            .find_group(group_id)
            .ok()
            .and_then(|s| s.leader_state)
            .map(|s| s.0)
    }

    pub async fn get_group_leader_node_id(&self, group_id: u64) -> Option<u64> {
        if let Ok(state) = self.router.find_group(group_id) {
            for (_, replica) in state.replicas {
                if matches!(state.leader_state, Some(v) if v.0 == replica.id) {
                    return Some(replica.node_id);
                }
            }
        }
        None
    }

    pub async fn assert_group_leader(&self, group_id: u64) -> u64 {
        for _ in 0..10000 {
            if let Some(leader) = self.get_group_leader(group_id).await {
                return leader;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} does not have a leader");
    }

    pub async fn group_remove_node(&self, group_id: u64, node_id: u64) -> Result<()> {
        if let Ok(state) = self.router.find_group(group_id) {
            for (_, replica) in state.replicas {
                if replica.node_id == node_id {
                    let mut c = self.group(group_id);
                    c.remove_group_replica(replica.id).await?;
                }
            }
        }
        Ok(())
    }

    pub fn get_group_epoch(&self, group_id: u64) -> Option<u64> {
        self.router.find_group(group_id).ok().map(|s| s.epoch)
    }

    pub async fn must_group_epoch(&self, group_id: u64) -> u64 {
        for _ in 0..1000 {
            if let Some(epoch) = self.get_group_epoch(group_id) {
                return epoch;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no such group {group_id} exists");
    }

    pub async fn assert_large_group_epoch(&self, group_id: u64, former_epoch: u64) -> u64 {
        for _ in 0..1000 {
            if let Some(epoch) = self.get_group_epoch(group_id) {
                if epoch > former_epoch {
                    return epoch;
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group epoch still less than or equals to {former_epoch}");
    }

    pub fn group_contains_shard(&self, group_id: u64, shard_id: u64) -> bool {
        if let Ok(state) = self.router.find_group_by_shard(shard_id) {
            if state.id == group_id {
                return true;
            }
        }
        false
    }

    pub async fn assert_group_contains_shard(&self, group_id: u64, shard_id: u64) {
        for _ in 0..10000 {
            if self.group_contains_shard(group_id, shard_id) {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is not contains shard {shard_id}");
    }

    pub async fn collect_migration_state(
        &self,
        group_id: u64,
        node_id: u64,
    ) -> Result<CollectMigrationStateResponse> {
        let node_addr = self.nodes.get(&node_id).unwrap();
        let client = node_client_with_retry(node_addr).await;
        let resp = client
            .root_heartbeat(HeartbeatRequest {
                timestamp: 0,
                piggybacks: vec![PiggybackRequest {
                    info: Some(piggyback_request::Info::CollectMigrationState(
                        CollectMigrationStateRequest { group: group_id },
                    )),
                }],
            })
            .await?;
        for resp in &resp.piggybacks {
            match resp.info.as_ref().unwrap() {
                piggyback_response::Info::SyncRoot(_)
                | piggyback_response::Info::CollectStats(_)
                | piggyback_response::Info::CollectScheduleState(_)
                | piggyback_response::Info::CollectGroupDetail(_) => {}
                piggyback_response::Info::CollectMigrationState(resp) => {
                    return Ok(resp.clone());
                }
            }
        }
        panic!("collect_migration_state have't received response");
    }

    pub async fn collect_replica_state(
        &self,
        group_id: u64,
        node_id: u64,
    ) -> Result<Option<ReplicaState>> {
        let node_addr = self.nodes.get(&node_id).unwrap();
        let client = node_client_with_retry(node_addr).await;
        let resp = client
            .root_heartbeat(HeartbeatRequest {
                timestamp: 0,
                piggybacks: vec![PiggybackRequest {
                    info: Some(piggyback_request::Info::CollectGroupDetail(
                        CollectGroupDetailRequest {
                            groups: vec![group_id],
                        },
                    )),
                }],
            })
            .await
            .unwrap();
        for resp in &resp.piggybacks {
            match resp.info.as_ref().unwrap() {
                piggyback_response::Info::SyncRoot(_)
                | piggyback_response::Info::CollectStats(_)
                | piggyback_response::Info::CollectScheduleState(_)
                | piggyback_response::Info::CollectMigrationState(_) => {}
                piggyback_response::Info::CollectGroupDetail(resp) => {
                    for state in &resp.replica_states {
                        if state.group_id == group_id {
                            return Ok(Some(state.clone()));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    pub async fn get_shard_desc(&self, co_desc: &CollectionDesc, key: &[u8]) -> Option<ShardDesc> {
        self.router
            .find_shard(co_desc.clone(), key)
            .ok()
            .map(|(_, shard)| shard)
    }

    pub async fn get_router_group_state(&self, group_id: u64) -> Option<RouterGroupState> {
        self.router.find_group(group_id).ok()
    }

    pub async fn find_router_group_state_by_key(
        &self,
        co_desc: &CollectionDesc,
        key: &[u8],
    ) -> Option<RouterGroupState> {
        let (_, shard) = self.router.find_shard(co_desc.clone(), key).ok()?;
        self.router.find_group_by_shard(shard.id).ok()
    }

    pub async fn assert_collection_ready(&self, co_desc: &CollectionDesc) {
        let mut ready_group: HashSet<u64> = HashSet::default();
        for i in 0..255u8 {
            for _ in 0..1000 {
                let state = match self.find_router_group_state_by_key(co_desc, &[i]).await {
                    Some(state) => state,
                    None => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };
                if ready_group.insert(state.id) {
                    self.assert_num_group_voters(state.id, 3).await;
                }
            }
        }
    }
}
