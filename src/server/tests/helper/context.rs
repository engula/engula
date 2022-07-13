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
use std::{collections::HashMap, thread};

use engula_server::{runtime::ExecutorOwner, AllocatorConfig, Config, NodeConfig, RaftConfig};
use tempdir::TempDir;

use super::client::node_client_with_retry;
use crate::helper::socket::next_avail_port;

#[allow(dead_code)]
pub struct TestContext {
    root_dir: TempDir,
    alloc_cfg: AllocatorConfig,
}

#[allow(dead_code)]
impl TestContext {
    pub fn new(prefix: &str) -> Self {
        let root_dir = TempDir::new(prefix).unwrap();
        TestContext {
            root_dir,
            alloc_cfg: AllocatorConfig::default(),
        }
    }

    pub fn next_listen_address(&self) -> String {
        format!("127.0.0.1:{}", next_avail_port())
    }

    pub fn disable_replica_balance(&mut self) {
        self.alloc_cfg.enable_replica_balance = false;
        self.alloc_cfg.enable_replica_balance = false;
    }

    pub fn disable_shard_balance(&mut self) {
        self.alloc_cfg.enable_shard_balance = false;
    }

    #[allow(dead_code)]
    pub fn spawn_server(&self, idx: usize, addr: &str, init: bool, join_list: Vec<String>) {
        self.spawn_server_with_cfg(idx, addr, init, join_list, self.alloc_cfg.clone());
    }

    #[allow(dead_code)]
    pub fn spawn_server_with_cfg(
        &self,
        idx: usize,
        addr: &str,
        init: bool,
        join_list: Vec<String>,
        cfg: AllocatorConfig,
    ) {
        let addr = addr.to_owned();
        let name = idx.to_string();
        let root_dir = self.root_dir.path().join(name);
        let cfg = Config {
            root_dir,
            addr,
            init,
            join_list,
            node: NodeConfig::default(),
            raft: RaftConfig {
                tick_interval_ms: 5,
                ..Default::default()
            },
            allocator: cfg,
        };
        thread::spawn(move || {
            let owner = ExecutorOwner::new(1);
            engula_server::run(cfg, owner.executor()).unwrap()
        });
    }

    /// Create a set of servers and bootstrap all of them.
    #[allow(dead_code)]
    pub async fn bootstrap_servers(&self, num_server: usize) -> HashMap<u64, String> {
        let mut nodes = HashMap::new();
        let mut root_addr = String::default();
        for i in 0..num_server {
            let next_addr = self.next_listen_address();
            if i == 0 {
                self.spawn_server(i + 1, &next_addr, true, vec![]);
                root_addr = next_addr.clone();
                node_client_with_retry(&next_addr).await;
            } else {
                // Join node one by one so that the node id is increment.
                self.spawn_server(i + 1, &next_addr, false, vec![root_addr.clone()]);
                node_client_with_retry(&next_addr).await;
            }
            let node_id = i as u64;
            nodes.insert(node_id, next_addr);
        }
        nodes
    }
}
