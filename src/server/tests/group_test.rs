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
#![feature(backtrace)]

mod helper;

use std::{collections::HashMap, thread, time::Duration};

use engula_api::server::v1::*;
use engula_client::RequestBatchBuilder;
use engula_server::runtime::ExecutorOwner;
use tempdir::TempDir;
use tracing::info;

use crate::helper::{
    client::{node_client_with_retry, GroupClient},
    runtime::block_on_current,
    socket::next_avail_port,
};

#[ctor::ctor]
fn init() {
    use std::{panic, process};
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        tracing::error!("{:#?}", panic_info);
        tracing::error!("{:#?}", std::backtrace::Backtrace::force_capture());
        process::exit(1);
    }));

    tracing_subscriber::fmt::init();
}

fn next_listen_address() -> String {
    format!("localhost:{}", next_avail_port())
}

fn spawn_server(name: &str, addr: &str, init: bool, join_list: Vec<String>) {
    let addr = addr.to_owned();
    let name = name.to_string();
    thread::spawn(move || {
        let owner = ExecutorOwner::new(1);
        let tmp_dir = TempDir::new(&name).unwrap().into_path();

        engula_server::run(owner.executor(), tmp_dir, addr, init, join_list).unwrap()
    });
}

#[test]
fn add_replica() {
    let node_1_addr = next_listen_address();
    spawn_server("add-replica-node-1", &node_1_addr, true, vec![]);

    let node_2_addr = next_listen_address();
    spawn_server(
        "add-replica-node-2",
        &node_2_addr,
        false,
        vec![node_1_addr.clone()],
    );

    block_on_current(async {
        let client_1 = node_client_with_retry(&node_1_addr).await;
        let client_2 = node_client_with_retry(&node_2_addr).await;

        let group_id = 0;
        let new_replica_id = 123;
        let node_1_id = 0;
        let node_2_id = 1; // FIXME(walter)

        let root_group = GroupDesc {
            id: group_id,
            ..Default::default()
        };

        // 1. create replica firstly
        client_2
            .create_replica(new_replica_id, root_group)
            .await
            .unwrap();

        // 2. add replica to group
        let req = RequestBatchBuilder::new(node_1_id)
            .add_replica(group_id, 7, new_replica_id, node_2_id)
            .build();
        let resps = client_1.batch_group_requests(req).await.unwrap();
        assert_eq!(resps.len(), 1);
        assert!(resps[0].error.is_none());

        // FIXME(walter) find a more efficient way to detect leader elections.
        thread::sleep(Duration::from_secs(2));
    });
}

async fn bootstrap_servers(prefix: &str, num_server: usize) -> HashMap<u64, String> {
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

#[test]
fn create_group_with_multi_replicas() {
    block_on_current(async {
        let nodes = bootstrap_servers("create-group", 4).await;
        let client_1 = node_client_with_retry(nodes.get(&0).unwrap()).await;
        let client_2 = node_client_with_retry(nodes.get(&1).unwrap()).await;
        let client_3 = node_client_with_retry(nodes.get(&2).unwrap()).await;
        let client_4 = node_client_with_retry(nodes.get(&3).unwrap()).await;

        let group_id = 100000000;
        let group_desc = GroupDesc {
            id: group_id,
            replicas: vec![
                ReplicaDesc {
                    id: 100,
                    node_id: 0,
                    role: ReplicaRole::Voter as i32,
                },
                ReplicaDesc {
                    id: 101,
                    node_id: 1,
                    role: ReplicaRole::Voter as i32,
                },
                ReplicaDesc {
                    id: 102,
                    node_id: 2,
                    role: ReplicaRole::Voter as i32,
                },
            ],
            ..Default::default()
        };

        // 1. create group
        client_1
            .create_replica(100, group_desc.clone())
            .await
            .unwrap();
        client_2
            .create_replica(101, group_desc.clone())
            .await
            .unwrap();
        client_3
            .create_replica(102, group_desc.clone())
            .await
            .unwrap();

        info!("create new replica 103");

        // 2. create single replica
        let empty_desc = GroupDesc {
            id: group_id,
            ..Default::default()
        };
        client_4.create_replica(103, empty_desc).await.unwrap();

        info!("add replica 103 to group 1");

        let mut group_client = GroupClient::new(nodes);
        let req = RequestBatchBuilder::new(0)
            .add_replica(group_id, 3, 103, 3)
            .build();
        group_client.group(req.requests[0].clone()).await.unwrap();

        // FIXME(walter) find a more efficient way to detect leader elections.
        thread::sleep(Duration::from_secs(2));
    });
}
