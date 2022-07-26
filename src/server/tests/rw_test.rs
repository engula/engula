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

use engula_api::server::v1::ReplicaRole;
use engula_client::{EngulaClient, Partition};
use tracing::info;

use crate::helper::{client::*, context::*, init::setup_panic_hook, runtime::*};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[test]
fn single_node_server() {
    let mut ctx = TestContext::new("rw_test__single_node_server");
    let node_1_addr = ctx.next_listen_address();
    ctx.spawn_server(1, &node_1_addr, true, vec![]);

    block_on_current(async {
        node_client_with_retry(&node_1_addr).await;

        let addrs = vec![node_1_addr];
        let client = EngulaClient::connect(addrs).await.unwrap();
        let db = client.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();

        let k = "book_name".as_bytes().to_vec();
        let v = "rust_in_actions".as_bytes().to_vec();
        co.put(k.clone(), v).await.unwrap();
        let r = co.get(k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == "rust_in_actions"));
    });
}

#[test]
fn cluster_put_and_get() {
    block_on_current(async {
        let mut ctx = TestContext::new("rw_test__cluster_put_and_get");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let app = c.app_client().await;

        let db = app.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        let k = "book_name".as_bytes().to_vec();
        let v = "rust_in_actions".as_bytes().to_vec();
        co.put(k.clone(), v).await.unwrap();
        let r = co.get(k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == "rust_in_actions"));
    });
}

#[test]
fn cluster_put_many_keys() {
    block_on_current(async {
        let mut ctx = TestContext::new("rw_test__cluster_put_and_get");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let app = c.app_client().await;

        let db = app.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        for i in 0..1000 {
            let k = format!("key-{i}").as_bytes().to_vec();
            let v = format!("value-{i}").as_bytes().to_vec();
            co.put(k.clone(), v).await.unwrap();
            let r = co.get(k).await.unwrap();
            let r = r.map(String::from_utf8);
            assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));
        }
    });
}

#[test]
fn operation_with_config_change() {
    block_on_current(async {
        let mut ctx = TestContext::new("rw_test__operation_with_config_change");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let root_addr = nodes.get(&0).unwrap().clone();
        let c = ClusterClient::new(nodes).await;
        let app = c.app_client().await;

        let db = app.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        for i in 0..3000 {
            if i == 20 {
                ctx.stop_server(2).await;
                ctx.add_server(root_addr.clone(), 3).await;
            }

            let k = format!("key-{i}").as_bytes().to_vec();
            let v = format!("value-{i}").as_bytes().to_vec();
            co.put(k.clone(), v).await.unwrap();
            let r = co.get(k).await.unwrap();
            let r = r.map(String::from_utf8);
            assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));
        }
    });
}

#[test]
fn operation_with_leader_transfer() {
    block_on_current(async move {
        let mut ctx = TestContext::new("rw_test__operation_with_leader_transfer");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let app = c.app_client().await;

        let db = app.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Range {}))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        for i in 0..1000 {
            let k = format!("key-{i}").as_bytes().to_vec();
            let v = format!("value-{i}").as_bytes().to_vec();
            co.put(k.clone(), v).await.unwrap();
            let r = co.get(k.clone()).await.unwrap();
            let r = r.map(String::from_utf8);
            assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));

            if i % 100 == 0 {
                let state = c
                    .find_router_group_state_by_key(&co.desc(), k.as_slice())
                    .await
                    .unwrap();
                let leader_id = state.leader_state.unwrap().0;
                for (id, replica) in state.replicas {
                    if id != leader_id && replica.role == ReplicaRole::Voter as i32 {
                        info!(
                            "transfer leadership of group {} from {} to {}",
                            state.id, leader_id, id
                        );
                        let mut client = c.group(state.id);
                        client.transfer_leader(id).await.unwrap();
                        break;
                    }
                }
            }
        }
    });
}

#[test]
fn operation_with_shard_migration() {
    block_on_current(async move {
        let mut ctx = TestContext::new("rw_test__operation_with_shard_migration");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let app = c.app_client().await;

        let db = app.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Range {}))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        let source_state = c
            .find_router_group_state_by_key(&co.desc(), &[0])
            .await
            .unwrap();
        let prev_group_id = source_state.id;
        let target_group_id = 0;

        for i in 0..1000 {
            let k = format!("key-{i}").as_bytes().to_vec();
            let v = format!("value-{i}").as_bytes().to_vec();
            co.put(k.clone(), v).await.unwrap();
            let r = co.get(k).await.unwrap();
            let r = r.map(String::from_utf8);
            assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));

            if i % 100 == 0 {
                let source_state = c
                    .find_router_group_state_by_key(&co.desc(), &[0])
                    .await
                    .unwrap();
                if source_state.id == target_group_id {
                    continue;
                }
                let shard_desc = c.get_shard_desc(&co.desc(), &[0]).await.unwrap();
                let mut client = c.group(target_group_id);
                spawn(async move {
                    client
                        .accept_shard(source_state.id, source_state.epoch, &shard_desc)
                        .await
                        .unwrap();
                });
            }
        }
        let source_state = c
            .find_router_group_state_by_key(&co.desc(), &[0])
            .await
            .unwrap();
        assert_ne!(source_state.id, prev_group_id);
    });
}
