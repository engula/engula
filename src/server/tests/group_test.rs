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
fn add_replica() {
    block_on_current(async {
        let mut ctx = TestContext::new("add-replica");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(2).await;
        let c = ClusterClient::new(nodes).await;

        let group_id = 0;
        let new_replica_id = 123;

        let root_group = GroupDesc {
            id: group_id,
            ..Default::default()
        };

        info!("create new replica {new_replica_id} of group {group_id}");

        // 1. create replica firstly
        c.create_replica(1, new_replica_id, root_group).await;

        info!("try add replica {new_replica_id} into group {group_id}");

        // 2. add replica to group
        let mut group_client = c.group(group_id);
        group_client.add_replica(new_replica_id, 1).await.unwrap();

        c.assert_group_contains_member(group_id, new_replica_id)
            .await;
    });
}

#[test]
fn create_group_with_multi_replicas() {
    block_on_current(async {
        let mut ctx = TestContext::new("create-group-with-multi-replicas");
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

        info!("add replica 103 to group 1");

        let mut group_client = c.group(group_id);
        group_client.add_replica(103, 3).await.unwrap();
        c.assert_group_contains_member(group_id, 103).await;
    });
}

/// The root group can be promoted to cluster mode as long as enough nodes are added to the cluster.
#[test]
fn promote_to_cluster_from_single_node() {
    block_on_current(async {
        let mut ctx = TestContext::new("promote-to-cluster");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;

        let root_group_id = 0;
        for _ in 0..10000 {
            let members = c.group_members(root_group_id).await;
            if members
                .into_iter()
                .filter(|(_, v)| *v == ReplicaRole::Voter as i32)
                .count()
                == 3
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("could not promote root group to cluster");
    });
}

#[test]
fn cure_group() {
    block_on_current(async {
        let mut ctx = TestContext::new("cure-group");
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
                    id: 103,
                    node_id: 3,
                    role: ReplicaRole::Voter as i32,
                },
            ],
            ..Default::default()
        };

        // 1. create group
        c.create_replica(0, 100, group_desc.clone()).await;
        c.create_replica(1, 101, group_desc.clone()).await;
        c.create_replica(3, 103, group_desc.clone()).await;
        c.assert_group_leader(group_id).await;

        info!("shutdown node 3 and replica 103");
        c.assert_group_contains_member(group_id, 103).await;
        ctx.stop_server(3).await;

        info!("wait curing group {group_id}");
        c.assert_group_not_contains_member(group_id, 103).await;
    });
}
