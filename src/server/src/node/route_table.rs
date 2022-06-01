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
    sync::{Arc, RwLock},
};

use super::Replica;
use crate::bootstrap::ROOT_GROUP_ID;

/// A structure support replica route queries.
#[allow(unused)]
#[derive(Clone)]
pub struct ReplicaRouteTable
where
    Self: Send + Sync,
{
    // FIXME(walter) more efficient implementation.
    replicas: Arc<RwLock<HashMap<u64, Arc<Replica>>>>,
}

// FIXME(walter) define.
#[allow(unused)]
pub struct RootReplica {}

#[allow(unused)]
impl ReplicaRouteTable {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        ReplicaRouteTable {
            replicas: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    /// Find the corresponding replica.
    pub fn find(&self, replica_id: u64) -> Option<Arc<Replica>> {
        self.replicas.read().unwrap().get(&replica_id).cloned()
    }

    pub fn find_root(&self) -> Option<RootReplica> {
        todo!()
    }

    pub fn current_root_replica(&self) -> Option<Arc<Replica>> {
        let replicas = self.replicas.read().unwrap();
        for replica in replicas.values() {
            if replica.group_id() == ROOT_GROUP_ID
            /* && TODO(zojw): is_leader */
            {
                return Some(replica.clone());
            }
        }
        None
    }

    pub fn update(&self, replica: Arc<Replica>) {
        let replica_id = replica.replica_id();
        self.replicas.write().unwrap().insert(replica_id, replica);
    }

    pub fn upsert_root(&self, root_replica: &RootReplica) {
        todo!()
    }

    pub fn remove(&self, replica_id: u64) {
        self.replicas.write().unwrap().remove(&replica_id);
    }
}

/// A structure support raft route table query.
#[allow(unused)]
#[derive(Clone)]
pub struct RaftRouteTable
where
    Self: Send + Sync, {}

#[allow(unused)]
impl RaftRouteTable {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        RaftRouteTable {}
    }

    pub fn find(&self, replica_id: u64) -> Option<()> {
        todo!("define raft sender")
    }

    pub fn upsert(&self, replica_id: u64, sender: ()) -> Option<()> {
        todo!()
    }

    pub fn delete(&self, replica_id: u64) {
        todo!()
    }
}
