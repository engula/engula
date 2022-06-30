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
use tracing::info;

use super::Replica;
use crate::{node::engine::SnapshotMode, serverpb::v1::*, Error, Result};

impl Replica {
    pub async fn fetch_shard_chunk(&self, shard_id: u64, last_key: &[u8]) -> Result<ShardChunk> {
        self.check_leader_early()?;

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
        for mut key_iter in snapshot.iter() {
            // NOTICE:  Only migrate first version.
            if let Some(entry) = key_iter.next() {
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
            }

            // TODO(walter) magic value
            if size > 64 * 1024 * 1024 {
                break;
            }
        }

        Ok(ShardChunk { data: kvs })
    }

    pub async fn ingest(&self, shard_id: u64, chunk: ShardChunk, forwarded: bool) -> Result<()> {
        use crate::node::engine::WriteBatch;

        if chunk.data.is_empty() {
            return Ok(());
        }

        // TODO(walter) return if migration already finished.
        // TODO(walter) check request epoch and shard id.
        self.check_leader_early()?;

        info!("ingest {} data for shard {}", chunk.data.len(), shard_id);
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
        self.raft_node.clone().propose(eval_result).await??;

        Ok(())
    }

    pub async fn delete_chunks(&self, shard_id: u64, keys: &[(Vec<u8>, u64)]) -> Result<()> {
        use crate::node::engine::WriteBatch;

        if keys.is_empty() {
            return Ok(());
        }

        // TODO(walter) check request epoch and shard id.
        self.check_leader_early()?;

        let mut wb = WriteBatch::default();
        info!("delete chunks with {} keys", keys.len());
        for (key, version) in keys {
            self.group_engine.delete(&mut wb, shard_id, key, *version)?;
        }

        let eval_result = EvalResult {
            batch: Some(WriteBatchRep {
                data: wb.data().to_owned(),
            }),
            op: None,
        };
        self.raft_node.clone().propose(eval_result).await??;

        Ok(())
    }

    pub async fn setup_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.migrate(desc, MigrationEvent::Setup).await
    }

    pub async fn enter_pulling_step(&self, desc: &MigrationDesc) -> Result<()> {
        self.migrate(desc, MigrationEvent::Ingest).await
    }

    pub async fn commit_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.migrate(desc, MigrationEvent::Commit).await
    }

    pub async fn abort_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.migrate(desc, MigrationEvent::Abort).await
    }

    pub async fn finish_migration(&self, desc: &MigrationDesc) -> Result<()> {
        self.migrate(desc, MigrationEvent::Apply).await
    }

    async fn migrate(&self, desc: &MigrationDesc, event: MigrationEvent) -> Result<()> {
        let _guard = self.take_write_acl_guard().await;

        {
            let epoch = desc.src_group_epoch;
            let group_id = self.info.group_id;
            let lease_state = self.lease_state.lock().unwrap();
            if !lease_state.is_ready_for_serving() {
                return Err(Error::NotLeader(group_id, lease_state.leader_descriptor()));
            } else if matches!(event, MigrationEvent::Setup) {
                if epoch < lease_state.descriptor.epoch {
                    // This migration needs to be rollback.
                    return Err(Error::EpochNotMatch(lease_state.descriptor.clone()));
                } else if let Some(existed_desc) = lease_state
                    .migration_state
                    .as_ref()
                    .and_then(|s| s.migration_desc.as_ref())
                {
                    if existed_desc == desc {
                        return Err(Error::ServiceIsBusy("already exists a migration request"));
                    } else {
                        // The migration has been prepared.
                        info!("migration already exists");
                        return Ok(());
                    }
                }
            } else {
                // Check epochs equals
                if lease_state
                    .migration_state
                    .as_ref()
                    .and_then(|s| s.migration_desc.as_ref())
                    .map(|d| d != desc)
                    .unwrap_or_default()
                {
                    todo!("migrate {:?} but migrate desc isn't matches", event);
                }
            }
        }

        let sync_op = SyncOp::migration(event, desc.clone());
        let eval_result = EvalResult {
            batch: None,
            op: Some(sync_op),
        };
        self.raft_node.clone().propose(eval_result).await??;

        Ok(())
    }
}
