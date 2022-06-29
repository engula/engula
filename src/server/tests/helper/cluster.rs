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

use engula_server::runtime::ExecutorOwner;
use tempdir::TempDir;

use super::client::node_client_with_retry;
use crate::helper::socket::next_avail_port;

pub fn next_listen_address() -> String {
    format!("localhost:{}", next_avail_port())
}

pub fn spawn_server(name: &str, addr: &str, init: bool, join_list: Vec<String>) {
    let addr = addr.to_owned();
    let name = name.to_string();
    thread::spawn(move || {
        let owner = ExecutorOwner::new(1);
        let tmp_dir = TempDir::new(&name).unwrap().into_path();

        engula_server::run(owner.executor(), tmp_dir, addr, init, join_list).unwrap()
    });
}

/// Create a set of servers and bootstrap all of them.
pub async fn bootstrap_servers(prefix: &str, num_server: usize) -> HashMap<u64, String> {
    let mut nodes = HashMap::new();
    let mut root_addr = String::default();
    for i in 0..num_server {
        let name = format!("{}-{}", prefix, i + 1);
        let next_addr = next_listen_address();
        if i == 0 {
            spawn_server(&name, &next_addr, true, vec![]);
            root_addr = next_addr.clone();
        } else {
            // Join node one by one so that the node id is increment.
            spawn_server(&name, &next_addr, false, vec![root_addr.clone()]);
            node_client_with_retry(&next_addr).await;
        }
        let node_id = i as u64;
        nodes.insert(node_id, next_addr);
    }
    nodes
}
