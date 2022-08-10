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
    sync::{Arc, Mutex},
    time::Instant,
};

use engula_api::server::v1::*;
use engula_client::Router;

use crate::{
    node::Replica,
    raftgroup::RaftGroupState,
    schedule::{
        event_source::{CommonEventSource, EventSource},
        scheduler::EventWaker,
    },
};

macro_rules! inherit_event_source {
    ($name: ident) => {
        impl EventSource for $name {
            fn bind(&self, waker: EventWaker) {
                let mut inner = self.inner.lock().unwrap();
                inner.core.bind(waker);
            }

            fn active_tasks(&self) -> HashSet<u64> {
                let mut inner = self.inner.lock().unwrap();
                inner.core.active_tasks()
            }

            fn watch(&self, task_id: u64) {
                let mut inner = self.inner.lock().unwrap();
                inner.core.watch(task_id)
            }
        }
    };
}

pub struct GroupProviders {
    pub node: Arc<NodeProvider>,
    pub descriptor: Arc<GroupDescProvider>,
    pub replica_states: Arc<ReplicaStatesProvider>,
    pub raft_state: Arc<RaftStateProvider>,
}

pub struct GroupDescProvider {
    inner: Mutex<GroupDescProviderInner>,
}

pub struct GroupDescProviderInner {
    core: CommonEventSource,
    desc: GroupDesc,
}

/// NodeEventSource provides the node updated events.
pub struct NodeProvider {
    router: Router,
}

pub struct ReplicaStatesProvider {
    inner: Mutex<ReplicaStatesProviderInner>,
}

#[derive(Default)]
struct ReplicaStatesProviderInner {
    core: CommonEventSource,
    replica_states: Vec<ReplicaState>,
}

pub struct RaftStateProvider {
    inner: Mutex<RaftStateProviderInner>,
}

struct RaftStateProviderInner {
    core: CommonEventSource,
    raft_state: RaftGroupState,
    lost_peers: HashMap<u64, Instant>,
}

impl GroupDescProvider {
    pub fn new(desc: GroupDesc) -> GroupDescProvider {
        GroupDescProvider {
            inner: Mutex::new(GroupDescProviderInner {
                core: CommonEventSource::new(),
                desc,
            }),
        }
    }

    pub fn replicas(&self) -> Vec<ReplicaDesc> {
        let inner = self.inner.lock().unwrap();
        inner.desc.replicas.clone()
    }

    pub fn descriptor(&self) -> GroupDesc {
        let inner = self.inner.lock().unwrap();
        inner.desc.clone()
    }

    pub fn update(&self, desc: GroupDesc) {
        let mut inner = self.inner.lock().unwrap();
        if inner.desc.epoch < desc.epoch {
            inner.desc = desc;
            inner.core.fire();
        }
    }
}

impl NodeProvider {
    pub fn new(router: Router) -> Self {
        NodeProvider { router }
    }

    pub fn num_online_nodes(&self) -> usize {
        self.router.total_nodes()
    }
}

impl ReplicaStatesProvider {
    fn new() -> Self {
        ReplicaStatesProvider {
            inner: Mutex::default(),
        }
    }

    pub fn replica_states(&self) -> Vec<ReplicaState> {
        self.inner.lock().unwrap().replica_states.clone()
    }

    pub fn update(&self, mut states: Vec<ReplicaState>) {
        states.sort_by_key(|r| r.replica_id);
        let mut inner = self.inner.lock().unwrap();
        if inner.replica_states != states {
            inner.replica_states = states;
            inner.core.fire();
        }
    }
}

impl RaftStateProvider {
    pub fn new() -> Self {
        RaftStateProvider {
            inner: Mutex::new(RaftStateProviderInner {
                core: CommonEventSource::new(),
                raft_state: RaftGroupState::default(),
                lost_peers: HashMap::default(),
            }),
        }
    }

    pub fn update(&self, state: RaftGroupState) {
        let mut inner = self.inner.lock().unwrap();
        let lost_peers = state
            .peers
            .iter()
            .filter(|(_, s)| s.might_lost)
            .map(|(id, _)| *id)
            .collect::<HashSet<_>>();
        inner.lost_peers.retain(|k, _| lost_peers.contains(k));
        for id in lost_peers {
            inner.lost_peers.entry(id).or_insert_with(Instant::now);
        }
        inner.raft_state = state;
        inner.core.fire();
    }

    pub fn lost_peers(&self) -> HashSet<u64> {
        let inner = self.inner.lock().unwrap();
        inner.lost_peers.keys().cloned().collect()
    }
}

impl GroupProviders {
    pub fn new(replica: Arc<Replica>, router: Router) -> Self {
        let desc = replica.descriptor();
        GroupProviders {
            node: Arc::new(NodeProvider::new(router)),
            descriptor: Arc::new(GroupDescProvider::new(desc)),
            replica_states: Arc::new(ReplicaStatesProvider::new()),
            raft_state: Arc::new(RaftStateProvider::new()),
        }
    }
}

inherit_event_source!(GroupDescProvider);
inherit_event_source!(ReplicaStatesProvider);
inherit_event_source!(RaftStateProvider);
