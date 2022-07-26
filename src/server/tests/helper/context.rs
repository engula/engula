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
use std::{collections::HashMap, thread, time::Duration};

use engula_server::{
    node::replica::{ReplicaConfig, ReplicaTestingKnobs},
    runtime::{ExecutorOwner, ShutdownNotifier},
    Config, NodeConfig, RaftConfig, RootConfig,
};
use tempdir::TempDir;
use tracing::info;

use super::client::node_client_with_retry;
use crate::helper::socket::next_avail_port;

#[allow(dead_code)]
pub struct TestContext {
    root_dir: TempDir,
    root_cfg: RootConfig,
    replica_knobs: ReplicaTestingKnobs,
    disable_group_promoting: bool,

    tick_interval_ms: u64,

    notifiers: HashMap<u64, ShutdownNotifier>,
    handles: HashMap<u64, std::thread::JoinHandle<()>>,
}

#[allow(dead_code)]
impl TestContext {
    pub fn new(prefix: &str) -> Self {
        let root_dir = TempDir::new(prefix).unwrap();
        TestContext {
            root_dir,
            disable_group_promoting: false,
            replica_knobs: ReplicaTestingKnobs::default(),
            root_cfg: RootConfig::default(),
            tick_interval_ms: 50,
            notifiers: HashMap::default(),
            handles: HashMap::default(),
        }
    }

    pub fn shutdown(&mut self) {
        let _ = std::mem::take(&mut self.notifiers);
        for (_, handle) in std::mem::take(&mut self.handles) {
            handle.join().unwrap_or_default();
        }
    }

    pub fn next_listen_address(&self) -> String {
        format!("127.0.0.1:{}", next_avail_port())
    }

    pub fn mut_replica_testing_knobs(&mut self) -> &mut ReplicaTestingKnobs {
        &mut self.replica_knobs
    }

    pub fn disable_replica_balance(&mut self) {
        self.root_cfg.enable_replica_balance = false;
    }

    pub fn disable_leader_balance(&mut self) {
        self.root_cfg.enable_leader_balance = false;
    }

    pub fn disable_shard_balance(&mut self) {
        self.root_cfg.enable_shard_balance = false;
    }

    pub fn disable_group_balance(&mut self) {
        self.root_cfg.enable_group_balance = false;
    }

    pub fn disable_all_balance(&mut self) {
        self.disable_replica_balance();
        self.disable_leader_balance();
        self.disable_shard_balance();
        self.disable_group_balance();
    }

    #[allow(dead_code)]
    pub fn spawn_server(&mut self, idx: usize, addr: &str, init: bool, join_list: Vec<String>) {
        self.spawn_server_with_cfg(idx, addr, init, join_list, self.root_cfg.clone());
    }

    #[allow(dead_code)]
    pub fn spawn_server_with_cfg(
        &mut self,
        idx: usize,
        addr: &str,
        init: bool,
        join_list: Vec<String>,
        root: RootConfig,
    ) {
        let addr = addr.to_owned();
        let name = idx.to_string();
        let root_dir = self.root_dir.path().join(name);
        let cfg = Config {
            root_dir,
            addr,
            init,
            join_list,
            node: NodeConfig {
                replica: ReplicaConfig {
                    testing_knobs: self.replica_knobs.clone(),
                    ..Default::default()
                },
                ..Default::default()
            },
            raft: RaftConfig {
                tick_interval_ms: self.tick_interval_ms,
                ..Default::default()
            },
            root,
        };
        let notifier = ShutdownNotifier::new();
        let shutdown = notifier.subscribe();
        let handle = thread::spawn(move || {
            let owner = ExecutorOwner::new(1);
            engula_server::run(cfg, owner.executor(), shutdown).unwrap();
        });
        self.notifiers.insert(idx as u64, notifier);
        self.handles.insert(idx as u64, handle);
    }

    /// Create a set of servers and bootstrap all of them.
    #[allow(dead_code)]
    pub async fn bootstrap_servers(&mut self, num_server: usize) -> HashMap<u64, String> {
        let mut nodes = HashMap::new();
        for i in 0..num_server {
            let next_addr = self.next_listen_address();
            let node_id = i as u64;
            nodes.insert(node_id, next_addr);
        }
        self.start_servers(nodes).await
    }

    pub async fn start_servers(&mut self, nodes: HashMap<u64, String>) -> HashMap<u64, String> {
        let root_addr = nodes
            .get(&0)
            .cloned()
            .expect("root addr is missed in start_server()");
        let mut keys = nodes.keys().cloned().collect::<Vec<_>>();
        keys.sort_unstable();
        for id in keys {
            let addr = nodes.get(&id).unwrap().clone();
            if id == 0 {
                self.spawn_server(id as usize, &addr, true, vec![]);
                node_client_with_retry(&addr).await;
            } else {
                // Join node one by one so that the node id is increment.
                self.spawn_server(id as usize, &addr, false, vec![root_addr.clone()]);
                node_client_with_retry(&addr).await;
            }
        }
        nodes
    }

    pub async fn add_server(&mut self, root_addr: String, id: u64) -> String {
        assert_ne!(id, 0);
        let addr = self.next_listen_address();
        self.spawn_server(id as usize, &addr, false, vec![root_addr]);
        node_client_with_retry(&addr).await;
        addr
    }

    pub async fn stop_server(&mut self, id: u64) {
        info!("stop server {id}");
        self.notifiers.remove(&id);
        if let Some(handle) = self.handles.remove(&id) {
            handle.join().unwrap_or_default();
        }
    }

    pub async fn wait_election_timeout(&self) {
        tokio::time::sleep(Duration::from_millis(self.tick_interval_ms * 6)).await;
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.shutdown();
    }
}
