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
use std::{sync::Arc, time::Duration};

use tracing::{debug, warn};

use super::super::Replica;
use crate::{
    runtime::{sync::WaitGroup, Executor, TaskPriority},
    serverpb::v1::SyncOp,
    Error, Result,
};

pub fn setup(executor: Executor, replica: Arc<Replica>, wait_group: WaitGroup) {
    let group_id = replica.replica_info().group_id;
    let tag = &group_id.to_le_bytes();
    executor.spawn(Some(tag), TaskPriority::Low, async move {
        purge_orphan_replica_main(replica).await;
    println!("purge replica exit");
        drop(wait_group);
    });
}

async fn purge_orphan_replica_main(replica: Arc<Replica>) {
    while replica.on_leader(false).await.is_ok() {
        let orphan_replica_id = match find_orphan_replica(replica.as_ref()).await {
            Ok(id) => id,
            Err(err) => {
                warn!("find orphan replica: {}", err);
                continue;
            }
        };

        // Replicate purge orphan replica command.
        let op = SyncOp::purge_replica(orphan_replica_id);
        if let Err(err) = replica.propose_sync_op(op).await {
            match &err {
                Error::NotLeader(_, _) => debug!(
                    "propose purge orphan replica sync op: not leader, replica {}",
                    orphan_replica_id
                ),
                _ => warn!(
                    "purge orphan replica sync op: {}, replica {}",
                    err, orphan_replica_id
                ),
            }
            continue;
        }

        if let Err(err) = remove_orphan_replica(replica.as_ref(), orphan_replica_id).await {
            warn!(
                "remove orphan replica: {}, replica {}",
                err, orphan_replica_id
            );
        }
    }
}

#[allow(unused)]
async fn find_orphan_replica(replica: &Replica) -> Result<u64> {
    // TODO(walter) find orphan replicas.
    loop {
        replica.on_leader(false).await?;
        crate::runtime::time::sleep(Duration::from_millis(100)).await;
    }
}

#[allow(unused)]
async fn remove_orphan_replica(replica: &Replica, orphan_replica_id: u64) -> Result<()> {
    todo!()
}
