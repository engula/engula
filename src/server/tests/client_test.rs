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

use std::time::Duration;

use engula_client::{AppError, ClientOptions, Partition};
use tracing::info;

use crate::helper::{client::*, context::*, init::setup_panic_hook, runtime::*};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[test]
fn to_unreachable_peers() {
    block_on_current(async {
        let mut ctx = TestContext::new("client_test__to_unreachable_peers");
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let opts = ClientOptions {
            connect_timeout: Some(Duration::from_millis(50)),
            timeout: Some(Duration::from_millis(200)),
        };
        let client = c.app_client_with_options(opts).await;
        let db = client.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();
        co.put(k.clone(), v).await.unwrap();
        let r = co.get(k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == "value"));

        info!("shutdown cluster");

        ctx.shutdown();
        let k = "key".as_bytes().to_vec();
        let v = "value-1".as_bytes().to_vec();
        assert!(matches!(
            co.put(k.clone(), v).await,
            Err(AppError::DeadlineExceeded(_))
        ));
    });
}

#[test]
fn create_duplicated_database_or_collection() {
    block_on_current(async {
        let mut ctx = TestContext::new("client_test__create_duplicated_database_or_collection");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let client = c.app_client().await;
        let db = client.create_database("test_db".to_string()).await.unwrap();
        assert!(matches!(
            client.create_database("test_db".to_string()).await,
            Err(AppError::AlreadyExists(_))
        ));
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();
        assert!(matches!(
            db.create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
                .await,
            Err(AppError::AlreadyExists(_))
        ));
        c.assert_collection_ready(&co.desc()).await;

        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();
        co.put(k.clone(), v).await.unwrap();
        let r = co.get(k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == "value"));
    });
}

#[test]
fn access_not_exists_database_or_collection() {
    block_on_current(async {
        let mut ctx = TestContext::new("client_test__access_not_exists_database_or_collection");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let client = c.app_client().await;
        assert!(matches!(
            client.open_database("test_db".to_string()).await,
            Err(AppError::NotFound(_))
        ));
        let db = client.create_database("test_db".to_string()).await.unwrap();
        assert!(matches!(
            db.open_collection("test_co".to_string()).await,
            Err(AppError::NotFound(_))
        ));
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        let k = "key".as_bytes().to_vec();
        let v = "value".as_bytes().to_vec();
        co.put(k.clone(), v).await.unwrap();
        let r = co.get(k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == "value"));
    });
}

#[test]
fn request_to_offline_leader() {
    block_on_current(async {
        let mut ctx = TestContext::new("client_test__request_to_offline_leader");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let client = c.app_client().await;
        let db = client.create_database("test_db".to_string()).await.unwrap();
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
            if i == 100 {
                let state = c
                    .find_router_group_state_by_key(&co.desc(), b"key")
                    .await
                    .unwrap();
                let node_id = c.get_group_leader_node_id(state.id).await.unwrap();
                ctx.stop_server(node_id).await;
            }
        }
    })
}
