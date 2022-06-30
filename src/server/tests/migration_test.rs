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

use std::{collections::HashMap, thread, time::Duration};

use engula_api::{server::v1::*, v1::PutRequest};
use tracing::info;

use crate::helper::{client::*, cluster::*, runtime::block_on_current};

#[ctor::ctor]
fn init() {
    use std::{panic, process};
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        tracing::error!("{:#?}", panic_info);
        tracing::error!("{:#?}", std::backtrace::Backtrace::force_capture());
        process::exit(1);
    }));

    tracing_subscriber::fmt::init();
}

async fn create_replica(
    nodes: &HashMap<u64, String>,
    desc: GroupDesc,
    replica_id: u64,
    node_id: u64,
) {
    let node_addr = nodes.get(&node_id).unwrap();
    let client = node_client_with_retry(node_addr).await;
    client.create_replica(replica_id, desc).await.unwrap();
}

async fn accept_shard(
    nodes: &HashMap<u64, String>,
    shard_desc: &ShardDesc,
    group_id: u64,
    src_group_id: u64,
    src_group_epoch: u64,
) {
    let mut c = GroupClient::new(group_id, nodes.clone());
    let req = AcceptShardRequest {
        src_group_id,
        src_group_epoch,
        shard_desc: Some(shard_desc.to_owned()),
    };
    c.accept_shard(req).await.unwrap();
}

async fn insert(
    nodes: &HashMap<u64, String>,
    group_id: u64,
    shard_id: u64,
    range: std::ops::Range<u64>,
) {
    let mut c = GroupClient::new(group_id, nodes.clone());
    for i in range {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        let req = PutRequest {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        };
        c.put(shard_id, req).await.unwrap();
    }
}

/// Migration test within groups which have only one member, shard is empty.
#[test]
fn single_replica_empty_shard_migration() {
    block_on_current(async {
        let nodes = bootstrap_servers("single-replica-empty-shard-migration", 2).await;
        let node_1_id = 0;
        let node_2_id = 1;
        let group_id_1 = 100000;
        let group_id_2 = 100001;
        let replica_1 = 1000000;
        let replica_2 = 2000000;
        let shard_id = 10000000;

        info!(
            "create group {} at node {} with replica {} and shard {}",
            group_id_1, node_1_id, replica_1, shard_id,
        );

        let shard_desc = ShardDesc {
            id: shard_id,
            collection_id: shard_id,
            partition: Some(shard_desc::Partition::Range(
                shard_desc::RangePartition::default(),
            )),
        };
        let replica_desc_1 = ReplicaDesc {
            id: replica_1,
            node_id: node_1_id,
            role: ReplicaRole::Voter as i32,
        };
        let group_desc_1 = GroupDesc {
            id: group_id_1,
            shards: vec![shard_desc.clone()],
            replicas: vec![replica_desc_1.clone()],
            ..Default::default()
        };
        create_replica(&nodes, group_desc_1.clone(), replica_1, node_1_id).await;

        info!(
            "create group {} at node {} with replica {}",
            group_id_2, node_2_id, replica_2
        );
        let replica_desc_2 = ReplicaDesc {
            id: replica_2,
            node_id: node_2_id,
            role: ReplicaRole::Voter as i32,
        };
        let group_desc_2 = GroupDesc {
            id: group_id_2,
            shards: vec![],
            replicas: vec![replica_desc_2.clone()],
            ..Default::default()
        };
        create_replica(&nodes, group_desc_2.clone(), replica_2, node_2_id).await;

        info!(
            "issue accept shard {} request to group {}",
            shard_id, group_id_2
        );

        // FIXME(walter) src group epoch
        accept_shard(&nodes, &shard_desc, group_id_2, group_id_1, 2).await;

        // FIXME(walter) find a more efficient way to detect migration finished.
        thread::sleep(Duration::from_secs(6));
    });
}

/// Migration test within groups which have only one member, shard have 1000 key values.
#[test]
fn single_replica_migration() {
    block_on_current(async {
        let nodes = bootstrap_servers("single-replica-migration", 2).await;
        let node_1_id = 0;
        let node_2_id = 1;
        let group_id_1 = 100000;
        let group_id_2 = 100001;
        let replica_1 = 1000000;
        let replica_2 = 2000000;
        let shard_id = 10000000;

        info!(
            "create group {} at node {} with replica {} and shard {}",
            group_id_1, node_1_id, replica_1, shard_id,
        );

        let shard_desc = ShardDesc {
            id: shard_id,
            collection_id: shard_id,
            partition: Some(shard_desc::Partition::Range(
                shard_desc::RangePartition::default(),
            )),
        };
        let replica_desc_1 = ReplicaDesc {
            id: replica_1,
            node_id: node_1_id,
            role: ReplicaRole::Voter as i32,
        };
        let group_desc_1 = GroupDesc {
            id: group_id_1,
            shards: vec![shard_desc.clone()],
            replicas: vec![replica_desc_1.clone()],
            ..Default::default()
        };
        create_replica(&nodes, group_desc_1.clone(), replica_1, node_1_id).await;

        info!("insert data into group {} shard {}", group_id_1, shard_id);
        insert(&nodes, group_id_1, shard_id, 0..1000).await;

        info!(
            "create group {} at node {} with replica {}",
            group_id_2, node_2_id, replica_2
        );
        let replica_desc_2 = ReplicaDesc {
            id: replica_2,
            node_id: node_2_id,
            role: ReplicaRole::Voter as i32,
        };
        let group_desc_2 = GroupDesc {
            id: group_id_2,
            shards: vec![],
            replicas: vec![replica_desc_2.clone()],
            ..Default::default()
        };
        create_replica(&nodes, group_desc_2.clone(), replica_2, node_2_id).await;

        info!(
            "issue accept shard {} request to group {}",
            shard_id, group_id_2
        );

        // FIXME(walter) src group epoch
        accept_shard(&nodes, &shard_desc, group_id_2, group_id_1, 2).await;

        // FIXME(walter) find a more efficient way to detect migration finished.
        thread::sleep(Duration::from_secs(6));
    });
}

async fn create_group(
    nodes: &HashMap<u64, String>,
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
        create_replica(nodes, group_desc.clone(), replica_id, node_id).await;
    }
}

// FIXME(walter) since the different replica has different column family id, the `WriteBatchRep`
// structure need to be update.
/// The basic migration test.
#[allow(unused)]
fn basic_migration() {
    block_on_current(async {
        let nodes = bootstrap_servers("basic-migration", 3).await;
        let node_1_id = 0;
        let node_2_id = 1;
        let node_3_id = 2;

        let group_id_1 = 100000;
        let group_id_2 = 100001;
        let replica_1_1 = 1000001;
        let replica_1_2 = 1000002;
        let replica_1_3 = 1000003;
        let replica_2_1 = 2000001;
        let replica_2_2 = 2000002;
        let replica_2_3 = 2000003;
        let shard_id = 10000000;

        info!("create group {} with shard {}", group_id_1, shard_id,);

        let shard_desc = ShardDesc {
            id: shard_id,
            collection_id: shard_id,
            partition: Some(shard_desc::Partition::Range(
                shard_desc::RangePartition::default(),
            )),
        };
        create_group(
            &nodes,
            group_id_1,
            vec![
                (replica_1_1, node_1_id),
                (replica_1_2, node_2_id),
                (replica_1_3, node_3_id),
            ],
            vec![shard_desc.clone()],
        )
        .await;

        info!("insert data into group {} shard {}", group_id_1, shard_id);
        insert(&nodes, group_id_1, shard_id, 0..1000).await;

        info!("create group {} ", group_id_2);
        create_group(
            &nodes,
            group_id_2,
            vec![
                (replica_2_1, node_1_id),
                (replica_2_2, node_2_id),
                (replica_2_3, node_3_id),
            ],
            vec![],
        )
        .await;
        info!(
            "issue accept shard {} request to group {}",
            shard_id, group_id_2
        );

        // FIXME(walter) src group epoch
        accept_shard(&nodes, &shard_desc, group_id_2, group_id_1, 4).await;

        // FIXME(walter) find a more efficient way to detect migration finished.
        thread::sleep(Duration::from_secs(30));
    });
}
