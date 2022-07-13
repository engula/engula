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

use engula_server::{runtime::ExecutorOwner, Config};
use tempdir::TempDir;

use super::client::node_client_with_retry;
use crate::helper::socket::next_avail_port;

#[allow(dead_code)]
pub struct TestContext {
    root_dir: TempDir,
}

#[allow(dead_code)]
impl TestContext {
    pub fn new(prefix: &str) -> Self {
        let root_dir = TempDir::new(prefix).unwrap();
        TestContext { root_dir }
    }

    pub fn next_listen_address(&self) -> String {
        format!("127.0.0.1:{}", next_avail_port())
    }

    #[allow(dead_code)]
    pub fn spawn_server(
        &self,
        idx: usize,
        addr: &str,
        init: bool,
        join_list: Vec<String>,
        cfg: Config,
    ) {
        let addr = addr.to_owned();
        let name = idx.to_string();
        let tmp_dir = self.root_dir.path().join(name);
        thread::spawn(move || {
            let owner = ExecutorOwner::new(1);
            engula_server::run(owner.executor(), tmp_dir, addr, init, join_list, cfg).unwrap()
        });
    }

    /// Create a set of servers and bootstrap all of them.
    #[allow(dead_code)]
    pub async fn bootstrap_servers(&self, num_server: usize, cfg: Config) -> HashMap<u64, String> {
        let mut nodes = HashMap::new();
        let mut root_addr = String::default();
        for i in 0..num_server {
            let next_addr = self.next_listen_address();
            if i == 0 {
                self.spawn_server(i + 1, &next_addr, true, vec![], cfg.clone());
                root_addr = next_addr.clone();
                node_client_with_retry(&next_addr).await;
            } else {
                // Join node one by one so that the node id is increment.
                self.spawn_server(
                    i + 1,
                    &next_addr,
                    false,
                    vec![root_addr.clone()],
                    cfg.clone(),
                );
                node_client_with_retry(&next_addr).await;
            }
            let node_id = i as u64;
            nodes.insert(node_id, next_addr);
        }
        nodes
    }
}
