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
mod helper;

use std::collections::HashSet;

use engula_api::server::v1::*;
use helper::context::TestContext;
use tracing::info;

use crate::helper::{client::*, init::setup_panic_hook, runtime::block_on_current};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

async fn create_group(c: &ClusterClient, group_id: u64, nodes: Vec<u64>, learners: Vec<u64>) {
    let learners = learners.into_iter().collect::<HashSet<_>>();
    let replicas = nodes
        .iter()
        .cloned()
        .map(|node_id| {
            let replica_id = group_id * 10 + node_id;
            ReplicaDesc {
                id: replica_id,
                node_id,
                role: if learners.contains(&node_id) {
                    ReplicaRole::Learner as i32
                } else {
                    ReplicaRole::Voter as i32
                },
            }
        })
        .collect::<Vec<_>>();
    let group_desc = GroupDesc {
        id: group_id,
        shards: vec![],
        replicas: replicas.clone(),
        ..Default::default()
    };
    for replica in replicas {
        c.create_replica(replica.node_id, replica.id, group_desc.clone())
            .await;
    }
}

#[test]
fn remove_orphan_replicas() {
    block_on_current(async {
        let mut ctx = TestContext::new("node-schedule-test--remove-orphan-replicas");
        ctx.mut_replica_testing_knobs()
            .disable_scheduler_orphan_replica_detecting_intervals = true;
        ctx.mut_replica_testing_knobs()
            .disable_scheduler_durable_task = true;
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
        c.assert_root_group_has_promoted().await;

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
                break;
            }
        }

        for _ in 0..1000 {
            if c.collect_replica_state(group_id, 3)
                .await
                .unwrap()
                .is_none()
            {
                return;
            }
        }
        panic!("replica 103 still exists in node 3");
    });
}

#[test]
fn remove_offline_learners() {
    block_on_current(async {
        let mut ctx = TestContext::new("node-schedule-test--remove-offline-learners");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes.clone()).await;

        let group_id = 10;

        info!("create new group {group_id}");
        let node_id_list = nodes.keys().cloned().collect();
        create_group(&c, group_id, node_id_list, vec![]).await;
        c.assert_group_leader(group_id).await;
        c.assert_root_group_has_promoted().await;
        let former_epoch = c.must_group_epoch(group_id).await;

        info!("add offline learners");
        let mut group = c.group(group_id);
        group.add_learner(123123, 123).await.unwrap();
        c.assert_large_group_epoch(group_id, former_epoch).await;

        ctx.wait_election_timeout().await;

        info!("remove offline learners");
        c.assert_group_not_contains_member(group_id, 123123).await;
    });
}

#[test]
fn remove_exceeds_offline_voters() {
    block_on_current(async {
        let mut ctx = TestContext::new("node-schedule-test--remove-exceeds-offline-voters");
        ctx.mut_replica_testing_knobs()
            .disable_scheduler_orphan_replica_detecting_intervals = true;
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes.clone()).await;

        let group_id = 10;

        info!("create new group {group_id}");
        let node_id_list = nodes.keys().cloned().collect();
        create_group(&c, group_id, node_id_list, vec![]).await;
        c.assert_group_leader(group_id).await;
        c.assert_root_group_has_promoted().await;
        let former_epoch = c.must_group_epoch(group_id).await;

        info!("add offline voters");
        let mut group = c.group(group_id);
        group.add_replica(123123, 123).await.unwrap();
        c.assert_large_group_epoch(group_id, former_epoch).await;

        ctx.wait_election_timeout().await;

        info!("remove offline voters");
        c.assert_group_not_contains_member(group_id, 123123).await;
    });
}

#[test]
fn remove_exceeds_online_voters() {
    block_on_current(async {
        let mut ctx = TestContext::new("node-schedule-test--remove-exceeds-online-voters");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(4).await;
        let c = ClusterClient::new(nodes.clone()).await;

        let group_id = 10;
        let node_id_list = nodes.keys().cloned().collect::<Vec<_>>();

        info!("create new group {group_id}");
        create_group(&c, group_id, node_id_list, vec![]).await;
        c.assert_group_leader(group_id).await;
        c.assert_root_group_has_promoted().await;

        ctx.wait_election_timeout().await;

        info!("remove exceeds online voters");
        c.assert_num_group_voters(group_id, 3).await;
    });
}

#[test]
fn replace_offline_voters_by_existing_learners() {
    block_on_current(async {
        let mut ctx =
            TestContext::new("node-schedule-test--replace-offline-voteres-by-existing-learners");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes.clone()).await;

        let group_id = 10;
        let node_id_list = nodes.keys().cloned().collect::<Vec<_>>();
        let learner_node_id = node_id_list.last().cloned().unwrap();

        info!("create new group {group_id} with learner on node {learner_node_id}");
        create_group(&c, group_id, node_id_list, vec![learner_node_id]).await;
        c.assert_group_leader(group_id).await;
        c.assert_root_group_has_promoted().await;
        let former_epoch = c.must_group_epoch(group_id).await;

        // FIXME(walter) the `learner_node_id` will be promote to voters before we add offline
        // replicas.
        info!("add offline voters");
        let mut group = c.group(group_id);
        group.add_replica(1235, 123).await.unwrap();
        c.assert_large_group_epoch(group_id, former_epoch).await;

        ctx.wait_election_timeout().await;

        info!("replace offline voters by online learners");
        c.assert_group_not_contains_member(group_id, 1235).await;
        c.assert_num_group_voters(group_id, 3).await;
    });
}

#[test]
fn supply_replicas_by_promoting_learners() {
    block_on_current(async {
        let mut ctx = TestContext::new("node-schedule-test--supply_replicas_by_promoting_learners");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes.clone()).await;

        let group_id = 10;
        let node_id_list = nodes.keys().cloned().collect::<Vec<_>>();
        let learner_node_id = node_id_list.last().cloned().unwrap();

        info!("create new group {group_id} with learner on node {learner_node_id}");
        create_group(&c, group_id, node_id_list, vec![learner_node_id]).await;
        c.assert_group_leader(group_id).await;
        c.assert_root_group_has_promoted().await;

        ctx.wait_election_timeout().await;

        info!("supply replicas by promoting learners");
        c.assert_num_group_voters(group_id, 3).await;
    });
}

#[test]
fn cure_group() {
    block_on_current(async {
        let mut ctx = TestContext::new("node-schedule-test--cure-group");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(4).await;
        let c = ClusterClient::new(nodes.clone()).await;

        let group_id = 10;
        let mut node_id_list = nodes.keys().cloned().collect::<Vec<_>>();
        node_id_list.pop();
        let offline_node_id = node_id_list.last().cloned().unwrap();

        info!("create new group {group_id}");
        create_group(&c, group_id, node_id_list, vec![]).await;
        c.assert_group_leader(group_id).await;
        c.assert_root_group_has_promoted().await;

        info!("stop server {offline_node_id}");
        c.assert_root_group_has_promoted().await;
        ctx.stop_server(offline_node_id).await;
        ctx.wait_election_timeout().await;
        c.assert_group_leader(group_id).await;

        info!("cure group by replace offline voters with new replicas");
        ctx.wait_election_timeout().await;
        c.assert_group_not_contains_node(group_id, offline_node_id)
            .await;
    });
}
