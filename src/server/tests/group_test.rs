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

use std::{thread, time::Duration};

use engula_api::server::v1::GroupDesc;
use engula_client::{NodeClient, RequestBatchBuilder};
use engula_server::runtime::ExecutorOwner;
use helper::socket::next_avail_port;
use tempdir::TempDir;

#[ctor::ctor]
fn init() {
    use std::{panic, process};
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        println!("{:?}", std::backtrace::Backtrace::force_capture());
        process::exit(1);
    }));

    tracing_subscriber::fmt::init();
}

fn next_listen_address() -> String {
    format!("localhost:{}", next_avail_port())
}

fn spawn_server(name: &'static str, addr: &str, init: bool, join_list: Vec<String>) {
    let addr = addr.to_owned();
    thread::spawn(move || {
        let owner = ExecutorOwner::new(1);
        let tmp_dir = TempDir::new(name).unwrap().into_path();

        engula_server::run(owner.executor(), tmp_dir, addr, init, join_list, None).unwrap()
    });
}

async fn node_client_with_retry(addr: &str) -> NodeClient {
    for _ in 0..100 {
        match NodeClient::connect(addr.to_string()).await {
            Ok(client) => return client,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
    }
    panic!("connect to {} timeout", addr);
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
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
            .add_replica(group_id, new_replica_id, node_2_id)
            .build();
        let resps = client_1.batch_group_requests(req).await.unwrap();
        assert_eq!(resps.len(), 1);
        assert!(resps[0].error.is_none());
    });
}
