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

use std::{net::SocketAddr, sync::mpsc, thread, time::Duration};

use engula_api::server::v1::GroupDesc;
use engula_client::{NodeClient, RequestBatchBuilder};
use engula_server::{runtime::ExecutorOwner, Result};
use tempdir::TempDir;

#[ctor::ctor]
fn init() {
    tracing_subscriber::fmt::init();
}

fn spawn_server(
    name: &'static str,
    init: bool,
    join_list: Vec<String>,
) -> mpsc::Receiver<SocketAddr> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let owner = ExecutorOwner::new(1);
        let tmp_dir = TempDir::new(name).unwrap().into_path();

        engula_server::run(
            owner.executor(),
            tmp_dir,
            "localhost:0".to_string(),
            init,
            join_list,
            Some(sender),
        )
        .unwrap()
    });
    receiver
}

#[test]
fn add_replica() {
    let node_1_addr = spawn_server("add-replica-node-1", true, vec![])
        .recv()
        .unwrap();
    let node_2_addr = spawn_server("add-replica-node-2", false, vec![node_1_addr.to_string()])
        .recv()
        .unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let client_1 = NodeClient::connect(node_1_addr.to_string()).await.unwrap();
        let client_2 = NodeClient::connect(node_2_addr.to_string()).await.unwrap();

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
