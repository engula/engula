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

use engula_api::server::v1::{GroupDesc, NodeDesc, ReplicaDesc};

use super::RootShared;
use crate::Result;

#[crate::async_trait]
pub trait AllocSource {
    async fn refresh_all(&self) -> Result<()>;

    fn nodes(&self) -> Vec<NodeDesc>;

    fn groups(&self) -> Vec<GroupDesc>;

    fn node_replicas(&self, node_id: &u64) -> Vec<ReplicaDesc>;
}

#[derive(Clone)]
pub struct SysAllocSource {
    root: Arc<RootShared>,

    nodes: Arc<Mutex<Vec<NodeDesc>>>,
    groups: Arc<Mutex<GroupInfo>>,
}

#[derive(Default)]
struct GroupInfo {
    descs: Vec<GroupDesc>,
    node_replicas: HashMap<u64, Vec<ReplicaDesc>>,
}

impl SysAllocSource {
    pub fn new(root: Arc<RootShared>) -> Self {
        Self {
            root,
            nodes: Default::default(),
            groups: Default::default(),
        }
    }
}

#[crate::async_trait]
impl AllocSource for SysAllocSource {
    async fn refresh_all(&self) -> Result<()> {
        self.reload_nodes().await?;
        self.reload_groups().await
    }

    fn nodes(&self) -> Vec<NodeDesc> {
        let nodes = self.nodes.lock().unwrap();
        nodes.to_owned()
    }

    fn groups(&self) -> Vec<GroupDesc> {
        let groups = self.groups.lock().unwrap();
        groups.descs.to_owned()
    }

    fn node_replicas(&self, node_id: &u64) -> Vec<ReplicaDesc> {
        let groups = self.groups.lock().unwrap();
        groups
            .node_replicas
            .get(node_id)
            .map(ToOwned::to_owned)
            .unwrap_or_default()
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
        _ = std::mem::replace(&mut *nodes, ns);
    }

    async fn reload_groups(&self) -> Result<()> {
        let schema = self.root.schema()?;
        let cur_groups = schema.list_group().await?;
        self.set_groups(cur_groups);
        Ok(())
    }

    fn set_groups(&self, gs: Vec<GroupDesc>) {
        let mut groups = self.groups.lock().unwrap();
        let mut node_replicas: HashMap<u64, Vec<ReplicaDesc>> = HashMap::new();
        for group in gs.iter() {
            for replica in &group.replicas {
                match node_replicas.entry(replica.node_id) {
                    Entry::Occupied(mut ent) => {
                        (*ent.get_mut()).push(replica.to_owned());
                    }
                    Entry::Vacant(ent) => {
                        ent.insert(vec![replica.to_owned()]);
                    }
                };
            }
        }
        _ = std::mem::replace(
            &mut *groups,
            GroupInfo {
                descs: gs,
                node_replicas,
            },
        );
    }
}
