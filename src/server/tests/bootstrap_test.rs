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

use engula_server::Result;

use crate::helper::{client::*, context::*, init::setup_panic_hook, runtime::block_on_current};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[test]
fn bootstrap_cluster() -> Result<()> {
    let mut ctx = TestContext::new("bootstrap-cluster");
    let node_1_addr = ctx.next_listen_address();
    ctx.spawn_server(1, &node_1_addr, true, vec![]);

    block_on_current(async {
        node_client_with_retry(&node_1_addr).await;
    });

    // At this point, initialization has been completed.
    Ok(())
}

#[test]
fn join_node() -> Result<()> {
    let mut ctx = TestContext::new("join-node");
    let node_1_addr = ctx.next_listen_address();
    ctx.spawn_server(1, &node_1_addr, true, vec![]);

    let node_2_addr = ctx.next_listen_address();
    ctx.spawn_server(2, &node_2_addr, false, vec![node_1_addr.clone()]);

    block_on_current(async {
        node_client_with_retry(&node_1_addr).await;
        node_client_with_retry(&node_2_addr).await;
    });

    // At this point, initialization and join has been completed.

    Ok(())
}

#[test]
fn restart_cluster() {
    block_on_current(async {
        let mut ctx = TestContext::new("bootstrap_test__restart_cluster");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;

        // Shutdown and restart servers.
        ctx.shutdown();

        let nodes = ctx
            .start_servers(nodes.get(&0).unwrap().to_owned(), nodes)
            .await;
        let c = ClusterClient::new(nodes).await;
        let app = c.app_client().await;
        app.create_database("db".into()).await.unwrap();
    });
}
