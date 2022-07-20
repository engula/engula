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

use engula_api::server::v1::*;
use helper::context::TestContext;
use tracing::info;

use crate::helper::{client::*, init::setup_panic_hook, runtime::block_on_current};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[test]
fn remove_orphan_replicas() {
    block_on_current(async {
        let mut ctx = TestContext::new("remove-orphan-replicas");
        ctx.mut_replica_testing_knobs()
            .disable_orphan_replica_detecting_intervals = true;
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(4).await;
        let c = ClusterClient::new(nodes).await;

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
        c.create_replica(0, 100, group_desc.clone()).await;
        c.create_replica(1, 101, group_desc.clone()).await;
        c.create_replica(2, 102, group_desc.clone()).await;
        c.assert_group_leader(group_id).await;

        info!("create new replica 103");

        // 2. create single replica
        let empty_desc = GroupDesc {
            id: group_id,
            ..Default::default()
        };
        c.create_replica(3, 103, empty_desc).await;

        info!("replica 103 should be removed because orphan replica");

        for _ in 0..1000 {
            if c.collect_replica_state(group_id, 3)
                .await
                .unwrap()
                .is_some()
            {
                return;
            }
        }
        panic!("replica 103 still exists in node 3");
    });
}
