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
use std::sync::Arc;

use tracing::{error, info};

use crate::{
    node::{engine::SnapshotMode, GroupEngine, Replica},
    runtime::{Executor, TaskPriority},
    Result,
};

// #[allow(unused)]
// pub async fn setup_remove_shard(
//     executor: &Executor,
//     replica: Arc<Replica>,
//     group_engine: GroupEngine,
//     shard_id: u64,
// ) {
//     let info = replica.replica_info();
//     let id = info.group_id.to_le_bytes();
//     let tag = Some(id.as_slice());
//     executor.spawn(tag, TaskPriority::IoLow, async move {
//         match remove_shard(replica, group_engine, shard_id).await {
//             Ok(()) => {
//                 info!(
//                     "replica {} remove shard {} success",
//                     info.replica_id, shard_id
//                 );
//                 // TODO(walter) change migration state.
//             }
//             Err(err) => {
//                 error!(
//                     "replica {} remove shard {}: {}",
//                     info.replica_id, shard_id, err
//                 );
//             }
//         }
//     });
// }

pub async fn remove_shard(
    replica: &Replica,
    group_engine: GroupEngine,
    shard_id: u64,
) -> Result<()> {
    let mut latest_key: Option<Vec<u8>> = None;
    loop {
        let chunk = collect_chunks(&group_engine, shard_id, latest_key.as_deref()).await?;
        if chunk.is_empty() {
            break;
        }
        latest_key = Some(chunk.last().unwrap().0.to_owned());
        replica.delete_chunks(shard_id, &chunk).await?;
    }
    Ok(())
}

async fn collect_chunks(
    group_engine: &GroupEngine,
    shard_id: u64,
    start_key: Option<&[u8]>,
) -> Result<Vec<(Vec<u8>, u64)>> {
    let snapshot_mode = SnapshotMode::Start { start_key };
    let mut snapshot = group_engine.snapshot(shard_id, snapshot_mode)?;
    let mut buf = Vec::with_capacity(256);
    for mvcc_iter in snapshot.iter() {
        buf.extend(mvcc_iter.map(|e| (e.user_key().to_owned(), e.version())));
        if buf.len() >= 256 {
            break;
        }
    }
    snapshot.status()?;
    Ok(buf)
}
