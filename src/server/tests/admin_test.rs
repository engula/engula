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

use engula_api::v1::{CollectionDesc, DatabaseDesc};
use engula_client::{ClientOptions, EngulaClient, NodeClient, Partition};
use engula_server::diagnosis;
use tracing::info;

use crate::helper::{context::*, init::setup_panic_hook, runtime::block_on_current};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[test]
fn balance_init_cluster() {
    block_on_current(async {
        let node_count = 4;
        let mut ctx = TestContext::new("db-col-mng-1");
        ctx.disable_all_balance();
        let start = tokio::time::Instant::now();
        let nodes = ctx.bootstrap_servers(node_count).await;
        let addrs = nodes.values().cloned().collect::<Vec<_>>();
        tokio::time::sleep(Duration::from_secs(10)).await;

        loop {
            let m = curr_metadata(addrs.to_owned()).await;
            if m.balanced {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let m = curr_metadata(addrs.to_owned()).await;
        let stats = m
            .nodes
            .iter()
            .map(|n| {
                let repls = n.replicas.iter().filter(|r| r.group != 0);
                let leaders = repls.to_owned().filter(|r| r.raft_role == 2);
                (n.id, repls.count(), leaders.count())
            })
            .collect::<Vec<_>>();
        info!("{stats:?}, balanced: {}", m.balanced);
        info!("init cluster balance takes {:?}", start.elapsed());
    })
}

#[test]
fn admin_delete() {
    block_on_current(async {
        let mut ctx = TestContext::new("db-col-mng-2");
        ctx.mut_replica_testing_knobs()
            .disable_scheduler_orphan_replica_detecting_intervals = true;
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(1).await;
        let addrs = nodes.values().cloned().collect::<Vec<_>>();
        let c = EngulaClient::new(ClientOptions::default(), addrs.to_owned())
            .await
            .unwrap();
        {
            let db = c.create_database("test1".into()).await.unwrap();
            let c1 = db
                .create_collection("test_co1".into(), Some(Partition::Hash { slots: 1 }))
                .await
                .unwrap();
            c1.put("k1".into(), "v1".into()).await.unwrap();
            db.delete_collection("test_co1".into()).await.unwrap();
            assert!(db.open_collection("test_co1".into()).await.is_err());
            db.create_collection("test_co1".into(), Some(Partition::Hash { slots: 1 }))
                .await
                .unwrap();
            let oc2 = db.open_collection("test_co1".into()).await.unwrap();
            assert!(oc2.get("k1".into()).await.unwrap().is_none())
        }
        {
            c.create_database("test_db1".into()).await.unwrap();
            let db1 = c.open_database("test_db1".into()).await.unwrap();
            db1.create_collection("co1".into(), Some(Partition::Hash { slots: 1 }))
                .await
                .unwrap();
            assert!(db1.list_collection().await.unwrap().len() == 1);
            c.delete_database("test_db1".into()).await.unwrap();
            assert!(c.open_database("test_db1".into()).await.is_err());
            c.create_database("test_db1".into()).await.unwrap();
            let od2 = c.open_database("test_db1".into()).await.unwrap();
            assert!(od2.list_collection().await.unwrap().is_empty());
        }
    })
}

#[test]
fn admin_basic() {
    block_on_current(async {
        let node_count = 4;
        let mut ctx = TestContext::new("db-col-mng-3");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(node_count).await;
        let addrs = nodes.values().cloned().collect::<Vec<_>>();

        let c = EngulaClient::new(ClientOptions::default(), addrs.to_owned())
            .await
            .unwrap();
        let sys_db = c.open_database("__system__".to_owned()).await.unwrap();
        let sys_db_col = sys_db.open_collection("database".to_owned()).await.unwrap();
        let sys_col_col = sys_db
            .open_collection("collection".to_owned())
            .await
            .unwrap();

        // test create database.
        let new_db_name = "db1".to_owned();
        let new_db_id = 2;
        let new_db = {
            let pcnt = c.list_database().await.unwrap().len();

            assert!(sys_db_col
                .get(new_db_name.as_bytes().to_owned())
                .await
                .unwrap()
                .is_none());

            let new_db = c.create_database(new_db_name.to_owned()).await.unwrap();

            assert!(c.list_database().await.unwrap().len() == pcnt + 1);

            use prost::Message;
            let db_bytes = sys_db_col
                .get(new_db_name.to_owned().into_bytes())
                .await
                .unwrap()
                .unwrap();
            let db_desc = DatabaseDesc::decode(&*db_bytes).unwrap();
            assert!(db_desc.id == new_db_id);

            new_db
        };

        // test create collection.
        let new_collection_name = "col1".to_owned();
        let _new_col = {
            let pcnt = new_db.list_collection().await.unwrap().len();

            assert!(sys_col_col
                .get(collection_key(new_db_id, &new_collection_name))
                .await
                .unwrap()
                .is_none());

            let new_col = new_db
                .create_collection(
                    new_collection_name.to_owned(),
                    Some(Partition::Hash { slots: 2 }),
                )
                .await
                .unwrap();

            assert!(new_db.list_collection().await.unwrap().len() == pcnt + 1);

            use prost::Message;
            let col_bytes = sys_col_col
                .get(collection_key(new_db_id, &new_collection_name))
                .await
                .unwrap()
                .unwrap();
            let col_desc = CollectionDesc::decode(&*col_bytes).unwrap();
            assert!(col_desc.id == 9);

            new_col
        };

        // check meta data api.
        let m = curr_metadata(addrs).await;
        let d = m
            .databases
            .iter()
            .find(|d| d.name == new_db_name)
            .expect("created database not found");
        d.collections
            .iter()
            .find(|c| c.name == new_collection_name)
            .expect("created collection not found");
        assert!(m.nodes.len() == node_count);
    })
}

fn collection_key(database_id: u64, collection_name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + collection_name.len());
    buf.extend_from_slice(database_id.to_le_bytes().as_slice());
    buf.extend_from_slice(collection_name.as_bytes());
    buf
}

async fn curr_metadata(nodes: Vec<String>) -> diagnosis::Metadata {
    let root_addr = find_root(nodes).await;
    let resp = reqwest::get(format!("http://{root_addr}/admin/metadata"))
        .await
        .unwrap();
    let content = resp.bytes().await.unwrap();
    let json_res = serde_json::from_slice(&content);
    json_res.unwrap_or_else(|_| panic!("decode json fail: {:?}", content))
}

async fn find_root(nodes: Vec<String>) -> String {
    for node in nodes {
        let n_cli = NodeClient::connect(node).await;
        if n_cli.is_err() {
            continue;
        }
        let n_cli = n_cli.unwrap();
        let roots = n_cli.get_root().await.unwrap();
        return roots.root_nodes[0].addr.to_owned();
    }
    panic!("no avaliable root")
}
