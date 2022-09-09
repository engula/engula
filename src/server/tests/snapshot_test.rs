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

use engula_api::{
    server::v1::{group_request_union::Request, *},
    v1::PutRequest,
};
use engula_client::RetryState;
use helper::context::TestContext;

use crate::helper::{client::*, init::setup_panic_hook, runtime::block_on_current};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

async fn create_group(c: &ClusterClient, group_id: u64, nodes: Vec<u64>, shards: Vec<ShardDesc>) {
    let replicas = nodes
        .iter()
        .cloned()
        .map(|node_id| {
            let replica_id = group_id * 10 + node_id;
            ReplicaDesc {
                id: replica_id,
                node_id,
                role: ReplicaRole::Voter as i32,
            }
        })
        .collect::<Vec<_>>();
    let group_desc = GroupDesc {
        id: group_id,
        shards,
        replicas: replicas.clone(),
        ..Default::default()
    };
    for replica in replicas {
        c.create_replica(replica.node_id, replica.id, group_desc.clone())
            .await;
    }
}

async fn insert(c: &ClusterClient, group_id: u64, shard_id: u64, range: std::ops::Range<u64>) {
    let mut c = c.group(group_id);
    for i in range {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        let put = PutRequest {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        };
        let req = Request::Put(ShardPutRequest {
            shard_id,
            put: Some(put),
        });

        let mut retry_state = RetryState::default();
        loop {
            match c.request(&req).await {
                Ok(_) => break,
                Err(err) => {
                    retry_state.retry(err).await.unwrap();
                }
            }
        }
    }
}

#[test]
fn send_snapshot() {
    block_on_current(async {
        let mut ctx = TestContext::new("snapshot_test__send_snapshot");
        ctx.disable_all_balance();
        ctx.mut_raft_testing_knobs()
            .force_new_peer_receiving_snapshot = true;
        let nodes = ctx.bootstrap_servers(4).await;
        let c = ClusterClient::new(nodes.clone()).await;

        let mut node_ids = nodes.keys().cloned().collect::<Vec<_>>();
        let left_node_id = node_ids.pop().unwrap();

        let group_id = 123;
        let shard_id = 234;
        let shard_desc = ShardDesc {
            id: shard_id,
            collection_id: shard_id,
            partition: Some(shard_desc::Partition::Range(
                shard_desc::RangePartition::default(),
            )),
        };
        create_group(&c, group_id, node_ids.clone(), vec![shard_desc]).await;
        insert(&c, group_id, shard_id, 1..100).await;
        ctx.wait_election_timeout().await;

        let new_replica_id = 123123123;
        let empty_desc = GroupDesc {
            id: group_id,
            ..Default::default()
        };
        c.create_replica(left_node_id, new_replica_id, empty_desc)
            .await;
        let mut group_client = c.group(group_id);
        group_client
            .add_replica(new_replica_id, left_node_id)
            .await
            .unwrap();

        ctx.wait_election_timeout().await;
        c.assert_root_group_has_promoted().await;

        // majority(4) == 3, force `new_replica_id` accept new entries.
        ctx.stop_server(node_ids.last().cloned().unwrap()).await;
        ctx.wait_election_timeout().await;
        insert(&c, group_id, shard_id, 100..110).await;
    });
}
