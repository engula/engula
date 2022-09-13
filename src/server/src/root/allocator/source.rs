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
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use engula_api::server::v1::*;

use super::RootShared;
use crate::{root::liveness::Liveness, Result};

pub enum NodeFilter {
    #[allow(dead_code)]
    Alive,
    Schedulable,
    NotDecommissioned,
}

#[crate::async_trait]
pub trait AllocSource {
    async fn refresh_all(&self) -> Result<()>;

    fn nodes(&self, filter: NodeFilter) -> Vec<NodeDesc>;

    fn groups(&self) -> HashMap<u64, GroupDesc>;

    fn node_replicas(&self, node_id: &u64) -> Vec<(ReplicaDesc, u64)>;

    fn replica_state(&self, replica_id: &u64) -> Option<ReplicaState>;

    fn replica_states(&self) -> Vec<ReplicaState>;
}

#[derive(Clone)]
pub struct SysAllocSource {
    root: Arc<RootShared>,
    liveness: Arc<Liveness>,

    nodes: Arc<Mutex<Vec<NodeDesc>>>,
    groups: Arc<Mutex<GroupInfo>>,
    replicas: Arc<Mutex<ReplicaInfo>>,
}

#[derive(Default)]
struct GroupInfo {
    descs: HashMap<u64, GroupDesc>,
    node_replicas: HashMap<u64, Vec<(ReplicaDesc, u64 /* group_id */)>>,
}

#[derive(Default)]
struct ReplicaInfo {
    replicas: HashMap<u64, ReplicaState>,
}

impl SysAllocSource {
    pub fn new(root: Arc<RootShared>, liveness: Arc<Liveness>) -> Self {
        Self {
            root,
            liveness,
            nodes: Default::default(),
            groups: Default::default(),
            replicas: Default::default(),
        }
    }
}

#[crate::async_trait]
impl AllocSource for SysAllocSource {
    async fn refresh_all(&self) -> Result<()> {
        self.reload_nodes().await?;
        self.reload_groups().await?;
        self.reload_replica_status().await
    }

    fn nodes(&self, filter: NodeFilter) -> Vec<NodeDesc> {
        let all_nodes = { self.nodes.lock().unwrap().clone() };
        match filter {
            NodeFilter::Alive => all_nodes
                .into_iter()
                .filter(|n| !self.liveness.get(&n.id).is_dead())
                .collect::<Vec<_>>(),
            NodeFilter::Schedulable => all_nodes
                .into_iter()
                .filter(|n| {
                    n.status == NodeStatus::Active as i32 && !self.liveness.get(&n.id).is_dead()
                })
                .collect::<Vec<_>>(),
            NodeFilter::NotDecommissioned => all_nodes
                .into_iter()
                .filter(|n| n.status != NodeStatus::Decommissioned as i32)
                .collect::<Vec<_>>(),
        }
    }

    fn groups(&self) -> HashMap<u64, GroupDesc> {
        let groups = self.groups.lock().unwrap();
        groups.descs.to_owned()
    }

    fn node_replicas(&self, node_id: &u64) -> Vec<(ReplicaDesc, u64)> {
        let groups = self.groups.lock().unwrap();
        groups
            .node_replicas
            .get(node_id)
            .map(ToOwned::to_owned)
            .unwrap_or_default()
    }

    fn replica_state(&self, replica_id: &u64) -> Option<ReplicaState> {
        let replica_info = self.replicas.lock().unwrap();
        replica_info.replicas.get(replica_id).map(ToOwned::to_owned)
    }

    fn replica_states(&self) -> Vec<ReplicaState> {
        let replica_info = self.replicas.lock().unwrap();
        replica_info
            .replicas
            .iter()
            .map(|e| e.1.to_owned())
            .collect()
    }
}

impl SysAllocSource {
    async fn reload_nodes(&self) -> Result<()> {
        let schema = self.root.schema()?;
        let cur_nodes = schema.list_node().await?;
        self.set_nodes(cur_nodes);
        Ok(())
    }

    fn set_nodes(&self, ns: Vec<NodeDesc>) {
        let mut nodes = self.nodes.lock().unwrap();
        let _ = std::mem::replace(&mut *nodes, ns);
    }

    async fn reload_groups(&self) -> Result<()> {
        let schema = self.root.schema()?;
        let cur_groups = schema.list_group().await?;
        self.set_groups(cur_groups);
        Ok(())
    }

    fn set_groups(&self, gs: Vec<GroupDesc>) {
        let mut groups = self.groups.lock().unwrap();
        let mut node_replicas: HashMap<u64, Vec<(ReplicaDesc, u64)>> = HashMap::new();
        for group in gs.iter() {
            for replica in &group.replicas {
                match node_replicas.entry(replica.node_id) {
                    Entry::Occupied(mut ent) => {
                        (*ent.get_mut()).push((replica.to_owned(), group.id.to_owned()));
                    }
                    Entry::Vacant(ent) => {
                        ent.insert(vec![(replica.to_owned(), group.id.to_owned())]);
                    }
                };
            }
        }
        let descs = gs.into_iter().map(|g| (g.id, g)).collect();
        let _ = std::mem::replace(
            &mut *groups,
            GroupInfo {
                descs,
                node_replicas,
            },
        );
    }

    async fn reload_replica_status(&self) -> Result<()> {
        let schema = self.root.schema()?;
        let replicas = schema.list_replica_state().await?;
        self.set_replica_states(replicas);
        Ok(())
    }

    #[allow(dead_code)]
    fn set_replica_states(&self, rs: Vec<ReplicaState>) {
        let mut replicas = self.replicas.lock().unwrap();
        let id_to_state = rs
            .into_iter()
            .map(|r| (r.replica_id, r))
            .collect::<HashMap<u64, ReplicaState>>();
        let _ = std::mem::replace(
            &mut *replicas,
            ReplicaInfo {
                replicas: id_to_state,
            },
        );
    }
}
