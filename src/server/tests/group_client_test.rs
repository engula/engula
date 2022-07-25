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

use engula_api::server::v1::{shard_desc, GroupDesc, ReplicaDesc, ReplicaRole, ShardDesc};

use crate::helper::{
    client::ClusterClient, context::*, init::setup_panic_hook, runtime::block_on_current,
};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[test]
fn request_to_offline_leader() {
    block_on_current(async {
        let mut ctx = TestContext::new("basic-migration");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;

        let node_1_id = 0;
        let node_2_id = 1;
        let node_3_id = 2;

        let group_id_1 = 100000;
        let group_id_2 = 200000;
        let replica_1_1 = 1000001;
        let replica_1_2 = 1000002;
        let replica_1_3 = 1000003;
        let replica_2_1 = 2000001;
        let replica_2_2 = 2000002;
        let replica_2_3 = 2000003;

        let shard_id = 10000000;

        let shard_desc = ShardDesc {
            id: shard_id,
            collection_id: shard_id,
            partition: Some(shard_desc::Partition::Range(
                shard_desc::RangePartition::default(),
            )),
        };
        create_group(
            &c,
            group_id_1,
            vec![
                (replica_1_1, node_1_id),
                (replica_1_2, node_2_id),
                (replica_1_3, node_3_id),
            ],
            vec![shard_desc.clone()],
        )
        .await;
        create_group(
            &c,
            group_id_2,
            vec![
                (replica_2_1, node_1_id),
                (replica_2_2, node_2_id),
                (replica_2_3, node_3_id),
            ],
            vec![shard_desc.clone()],
        )
        .await;

        let mut sc = c.group(group_id_1);
        sc.transfer_leader(replica_1_3).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        ctx.stop_server(node_3_id).await;

        // use accept_shard to simulate construct and use server's group_client
        // replace this with simple put when group_client move to engula client
        let mut tc = c.group(group_id_2);
        let src_group_epoch = c.get_group_epoch(group_id_1).unwrap_or(4);
        tc.accept_shard(group_id_1, src_group_epoch, &shard_desc)
            .await
            .unwrap();

        c.assert_group_contains_shard(group_id_2, shard_id).await;
    })
}

async fn create_group(
    c: &ClusterClient,
    id: u64,
    replica_ids: Vec<(u64, u64)>,
    shards: Vec<ShardDesc>,
) {
    let replicas = replica_ids
        .iter()
        .cloned()
        .map(|(id, node_id)| ReplicaDesc {
            id,
            node_id,
            role: ReplicaRole::Voter as i32,
        })
        .collect();
    let group_desc = GroupDesc {
        id,
        shards,
        replicas,
        ..Default::default()
    };
    for (replica_id, node_id) in replica_ids {
        c.create_replica(node_id, replica_id, group_desc.clone())
            .await;
    }
}
