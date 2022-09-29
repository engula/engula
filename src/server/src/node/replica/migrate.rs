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

use engula_api::server::v1::*;
use tracing::{debug, info};

use super::{LeaseState, Replica, ReplicaInfo};
use crate::{
    node::engine::{SnapshotMode, WriteBatch},
    serverpb::v1::*,
    Error, Result,
};

impl Replica {
    pub async fn fetch_shard_chunk(
        &self,
        shard_id: u64,
        last_key: &[u8],
        chunk_size: usize,
    ) -> Result<ShardChunk> {
        let _acl_guard = self.take_read_acl_guard().await;
        self.check_migrating_request_early(shard_id)?;

        let mut kvs = vec![];
        let mut size = 0;

        let snapshot_mode = SnapshotMode::Start {
            start_key: if last_key.is_empty() {
                None
            } else {
                Some(last_key)
            },
        };
        let mut snapshot = self.group_engine.snapshot(shard_id, snapshot_mode)?;
        for key_iter in snapshot.iter() {
            let mut key_iter = key_iter?;
            // NOTICE: Only migrate the first version.
            if let Some(entry) = key_iter.next() {
                let entry = entry?;
                if entry.user_key() == last_key {
                    continue;
                }
                let key: Vec<_> = entry.user_key().to_owned();
                let value: Vec<_> = match entry.value() {
                    Some(v) => v.to_owned(),
                    None => {
                        // Skip tombstone.
                        continue;
                    }
                };
                size += key.len() + value.len();
                kvs.push(ShardData {
                    key,
                    value,
                    version: super::eval::MIGRATING_KEY_VERSION,
                });
                if size > chunk_size {
                    break;
                }
            }
        }

        Ok(ShardChunk { data: kvs })
    }

    pub async fn ingest(&self, shard_id: u64, chunk: ShardChunk, forwarded: bool) -> Result<()> {
        if chunk.data.is_empty() {
            return Ok(());
        }

        let _acl_guard = self.take_read_acl_guard().await;
        self.check_migrating_request_early(shard_id)?;

        let mut wb = WriteBatch::default();
        for data in &chunk.data {
            self.group_engine
                .put(&mut wb, shard_id, &data.key, &data.value, data.version)?;
        }

        let sync_op = if !forwarded {
            Some(SyncOp::ingest(
                chunk.data.last().as_ref().unwrap().key.clone(),
            ))
        } else {
            None
        };

        let eval_result = EvalResult {
            batch: Some(WriteBatchRep {
                data: wb.data().to_owned(),
            }),
            op: sync_op,
        };
        self.raft_node.clone().propose(eval_result).await?;

        Ok(())
    }

    pub async fn delete_chunks(&self, shard_id: u64, keys: &[(Vec<u8>, u64)]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let _acl_guard = self.take_read_acl_guard().await;
        self.check_migrating_request_early(shard_id)?;

        let mut wb = WriteBatch::default();
        for (key, version) in keys {
            self.group_engine.delete(&mut wb, shard_id, key, *version)?;
        }

        let eval_result = EvalResult {
            batch: Some(WriteBatchRep {
                data: wb.data().to_owned(),
            }),
            op: None,
        };
        self.raft_node.clone().propose(eval_result).await?;

        Ok(())
    }

    pub async fn setup_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.update_migration_state(desc, MigrationEvent::Setup)
            .await
    }

    pub async fn enter_pulling_step(&self, desc: &MigrationDesc) -> Result<()> {
        self.update_migration_state(desc, MigrationEvent::Ingest)
            .await
    }

    pub async fn commit_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.update_migration_state(desc, MigrationEvent::Commit)
            .await
    }

    pub async fn abort_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.update_migration_state(desc, MigrationEvent::Abort)
            .await
    }

    pub async fn finish_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.update_migration_state(desc, MigrationEvent::Apply)
            .await
    }

    async fn update_migration_state(
        &self,
        desc: &MigrationDesc,
        event: MigrationEvent,
    ) -> Result<()> {
        debug!(replica = self.info.replica_id,
            group = self.info.group_id,
            %desc,
            ?event,
            "update migration state");

        let _guard = self.take_write_acl_guard().await;
        if !self.check_migration_state_update_early(desc, event)? {
            return Ok(());
        }

        let sync_op = SyncOp::migration(event, desc.clone());
        let eval_result = EvalResult {
            batch: None,
            op: Some(sync_op),
        };
        self.raft_node.clone().propose(eval_result).await?;

        Ok(())
    }

    pub fn check_migrating_request_early(&self, shard_id: u64) -> Result<()> {
        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.is_ready_for_serving() {
            Err(Error::NotLeader(
                self.info.group_id,
                lease_state.applied_term,
                lease_state.leader_descriptor(),
            ))
        } else if !lease_state.is_migrating_shard(shard_id) {
            Err(Error::ShardNotFound(shard_id))
        } else {
            Ok(())
        }
    }

    fn check_migration_state_update_early(
        &self,
        desc: &MigrationDesc,
        event: MigrationEvent,
    ) -> Result<bool> {
        let group_id = self.info.group_id;

        let lease_state = self.lease_state.lock().unwrap();
        if !lease_state.is_ready_for_serving() {
            Err(Error::NotLeader(
                group_id,
                lease_state.applied_term,
                lease_state.leader_descriptor(),
            ))
        } else if matches!(event, MigrationEvent::Setup) {
            Self::check_migration_setup(self.info.as_ref(), &lease_state, desc)
        } else if matches!(event, MigrationEvent::Commit) {
            Self::check_migration_commit(self.info.as_ref(), &lease_state, desc)
        } else if lease_state.migration_state.is_none() {
            Err(Error::InvalidArgument(
                "no such migration exists".to_owned(),
            ))
        } else if !lease_state.is_same_migration(desc) {
            Err(Error::InvalidArgument(
                "exists another migration".to_owned(),
            ))
        } else {
            Ok(true)
        }
    }

    fn check_migration_setup(
        info: &ReplicaInfo,
        lease_state: &LeaseState,
        desc: &MigrationDesc,
    ) -> Result<bool> {
        let epoch = desc.src_group_epoch;
        if epoch < lease_state.descriptor.epoch {
            // This migration needs to be rollback.
            Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
        } else if lease_state.migration_state.is_none() {
            debug_assert_eq!(epoch, lease_state.descriptor.epoch);
            Ok(true)
        } else if !lease_state.is_same_migration(desc) {
            // This migration needs to be rollback too, because the epoch will be bumped once the
            // former migration finished.
            Err(Error::EpochNotMatch(lease_state.descriptor.clone()))
        } else {
            info!(
                replica = info.replica_id,
                group = info.group_id,
                %desc,
                "the same migration already exists");
            Ok(false)
        }
    }

    fn check_migration_commit(
        info: &ReplicaInfo,
        lease_state: &LeaseState,
        desc: &MigrationDesc,
    ) -> Result<bool> {
        if is_migration_finished(info, desc, &lease_state.descriptor) {
            info!(
                replica = info.replica_id,
                group = info.group_id,
                %desc,
                "this migration has been committed, skip commit request");
            Ok(false)
        } else if lease_state.migration_state.is_none() || !lease_state.is_same_migration(desc) {
            info!(
                "migration state is {:?}, descriptor {:?}",
                lease_state.migration_state, lease_state.descriptor
            );
            Err(Error::InvalidArgument(
                "no such migration exists".to_owned(),
            ))
        } else if lease_state.migration_state.as_ref().unwrap().step
            == MigrationStep::Migrated as i32
        {
            info!(
                replica = info.replica_id,
                group = info.group_id,
                %desc,
                "this migration has been committed, skip commit request");
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

fn is_migration_finished(info: &ReplicaInfo, desc: &MigrationDesc, descriptor: &GroupDesc) -> bool {
    let shard_desc = desc.shard_desc.as_ref().unwrap();
    if desc.src_group_id == info.group_id
        && desc.src_group_epoch < descriptor.epoch
        && is_shard_migrated_out(shard_desc, descriptor)
    {
        return true;
    }

    if desc.dest_group_id == info.group_id
        && desc.dest_group_epoch < descriptor.epoch
        && is_shard_migrated_in(shard_desc, descriptor)
    {
        return true;
    }

    false
}

fn is_shard_migrated_out(shard_desc: &ShardDesc, group_desc: &GroupDesc) -> bool {
    // For source dest, if a shard is migrated, the shard desc should not exists.
    for shard in &group_desc.shards {
        if shard.id == shard_desc.id {
            return false;
        }
    }
    true
}

fn is_shard_migrated_in(shard_desc: &ShardDesc, group_desc: &GroupDesc) -> bool {
    // For dest dest, if a shard is migrated, the shard desc should exists.
    for shard in &group_desc.shards {
        if shard.id == shard_desc.id {
            return true;
        }
    }
    false
}
