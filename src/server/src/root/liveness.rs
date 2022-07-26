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
    collections::{hash_map, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::Config;

#[derive(Clone)]
pub struct NodeLiveness {
    expiration: u128,
}

impl NodeLiveness {
    pub fn is_dead(&self) -> bool {
        self.expiration < current_timestamp()
    }

    #[allow(dead_code)]
    pub fn is_alive(&self) -> bool {
        self.expiration > current_timestamp()
    }
}

#[derive(Clone)]
pub struct Liveness {
    pub liveness_threshold: Duration,
    pub heartbeat_timeout: Duration,

    nodes: Arc<Mutex<HashMap<u64, NodeLiveness>>>,
}

impl Liveness {
    pub fn new(cfg: Config) -> Self {
        const RAFT_ELECTION_TIMEOUT_MULTIPLIER: u64 = 2;
        const LIVENESS_FRAC: f64 = 0.5;

        // ensure liveness_threshold >>> raft_election_timeout?
        let liveness_threshold = Duration::from_millis(std::cmp::max(
            cfg.raft.election_tick as u64
                * cfg.raft.tick_interval_ms
                * RAFT_ELECTION_TIMEOUT_MULTIPLIER,
            3000,
        ));
        let heartbeat_timeout =
            Duration::from_millis((liveness_threshold.as_millis() as f64 * LIVENESS_FRAC) as u64);
        Self {
            liveness_threshold,
            heartbeat_timeout,
            nodes: Default::default(),
        }
    }

    pub fn heartbeat_interval(&self) -> Duration {
        const RECONCILE_TIMEOUT: Duration = Duration::from_secs(1);
        self.liveness_threshold - self.heartbeat_timeout - RECONCILE_TIMEOUT /* TODO: reconcile
                                                                              * block job should
                                                                              * be async and not
                                                                              * affect the
                                                                              * hearbeat tick */
    }

    pub fn get(&self, node: &u64) -> NodeLiveness {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(node).cloned().unwrap_or_else(|| NodeLiveness {
            expiration: self.new_expiration(),
        })
    }

    pub fn renew(&self, node_id: u64) {
        let mut nodes = self.nodes.lock().unwrap();
        let entry = nodes.entry(node_id);
        match entry {
            hash_map::Entry::Occupied(mut ent) => {
                let renew = self.new_expiration();
                let ent = ent.get_mut();
                if ent.expiration < renew {
                    ent.expiration = renew
                }
            }
            hash_map::Entry::Vacant(ent) => {
                ent.insert(NodeLiveness {
                    expiration: self.new_expiration(),
                });
            }
        }
    }

    pub fn init_node_if_first_seen(&self, node_id: u64) {
        // Give `liveness_threshold` time window to retry before mark as offline.
        let mut nodes = self.nodes.lock().unwrap();
        if let hash_map::Entry::Vacant(ent) = nodes.entry(node_id) {
            ent.insert(NodeLiveness {
                expiration: self.new_expiration(),
            });
        }
    }

    pub fn reset(&self) {
        self.nodes.lock().unwrap().clear();
    }

    fn new_expiration(&self) -> u128 {
        current_timestamp() + self.liveness_threshold.as_millis()
    }
}

fn current_timestamp() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    since_the_epoch.as_millis()
}
