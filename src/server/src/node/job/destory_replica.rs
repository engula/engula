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

use tracing::error;

use crate::{
    node::{GroupEngine, StateEngine},
    runtime::TaskPriority,
    serverpb::v1::ReplicaLocalState,
    Provider, Result,
};

/// Clean a group engine and save the replica state to `ReplicaLocalState::Tombstone`.
pub(crate) fn setup(group_id: u64, replica_id: u64, provider: &Provider) {
    let tag = &group_id.to_le_bytes();
    let state_engine = provider.state_engine.clone();
    let raw_db = provider.raw_db.clone();
    provider
        .executor
        .spawn(Some(tag), TaskPriority::IoLow, async move {
            if let Err(err) = destory_replica(group_id, replica_id, state_engine, raw_db).await {
                error!("destory group engine: {}, group {}", err, group_id);
            }
        });
}

async fn destory_replica(
    group_id: u64,
    replica_id: u64,
    state_engine: StateEngine,
    raw_db: Arc<rocksdb::DB>,
) -> Result<()> {
    GroupEngine::destory(group_id, raw_db).await?;
    state_engine
        .save_replica_state(group_id, replica_id, ReplicaLocalState::Tombstone)
        .await?;
    Ok(())
}
