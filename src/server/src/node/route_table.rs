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
    task::Waker,
};

use super::{replica::RaftSender, Replica};
use crate::bootstrap::ROOT_GROUP_ID;

/// A structure support replica route queries.
#[derive(Clone)]
pub struct ReplicaRouteTable
where
    Self: Send + Sync,
{
    // FIXME(walter) more efficient implementation.
    core: Arc<RwLock<ReplicaRouteTableCore>>,
}

#[derive(Default)]
struct ReplicaRouteTableCore {
    replicas: HashMap<u64, Arc<Replica>>,
    root_wakers: Vec<Waker>,
}

impl ReplicaRouteTable {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        ReplicaRouteTable {
            core: Arc::default(),
        }
    }

    /// Find the corresponding replica.
    pub fn find(&self, group_id: u64) -> Option<Arc<Replica>> {
        let core = self.core.read().unwrap();
        core.replicas.get(&group_id).cloned()
    }

    pub fn current_root_replica(&self, waker: Option<Waker>) -> Option<Arc<Replica>> {
        let mut core = self.core.write().unwrap();
        if let Some(replica) = core.replicas.get(&ROOT_GROUP_ID) {
            return Some(replica.clone());
        }
        if let Some(waker) = waker {
            core.root_wakers.push(waker);
        }
        None
    }

    pub fn update(&self, replica: Arc<Replica>) {
        let group_id = replica.group_id();
        let mut core = self.core.write().unwrap();
        core.replicas.insert(group_id, replica);
        if group_id == ROOT_GROUP_ID {
            for waker in std::mem::take(&mut core.root_wakers) {
                waker.wake();
            }
        }
    }

    pub fn remove(&self, replica_id: u64) {
        let mut core = self.core.write().unwrap();
        core.replicas.remove(&replica_id);
    }
}

/// A structure support raft route table query.
#[derive(Clone)]
pub struct RaftRouteTable
where
    Self: Send + Sync,
{
    // FIXME(walter) more efficient implementation.
    senders: Arc<RwLock<HashMap<u64, RaftSender>>>,
}

impl RaftRouteTable {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        RaftRouteTable {
            senders: Arc::default(),
        }
    }

    pub fn find(&self, replica_id: u64) -> Option<RaftSender> {
        self.senders.read().unwrap().get(&replica_id).cloned()
    }

    pub fn update(&self, replica_id: u64, sender: RaftSender) {
        self.senders.write().unwrap().insert(replica_id, sender);
    }

    pub fn delete(&self, replica_id: u64) {
        self.senders.write().unwrap().remove(&replica_id);
    }
}
