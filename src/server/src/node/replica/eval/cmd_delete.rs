use engula_api::server::v1::ShardDeleteRequest;

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
use crate::{
    node::{
        engine::{GroupEngine, SnapshotMode, WriteBatch},
        migrate::ForwardCtx,
        replica::ExecCtx,
    },
    serverpb::v1::{EvalResult, WriteBatchRep},
    Error, Result,
};

pub async fn delete(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &ShardDeleteRequest,
) -> Result<EvalResult> {
    let delete = req
        .delete
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("ShardDeleteRequest::delete is None".into()))?;

    if let Some(desc) = exec_ctx.migration_desc.as_ref() {
        let shard_id = desc.shard_desc.as_ref().unwrap().id;
        if shard_id == req.shard_id {
            let forward_ctx = ForwardCtx {
                shard_id,
                dest_group_id: desc.dest_group_id,
                payloads: vec![],
            };
            return Err(Error::Forward(forward_ctx));
        }
    }

    let mut wb = WriteBatch::default();
    if exec_ctx.forward_shard_id.is_some() {
        // Write tombstone for migrating shard, so that the a deleted key will be overwrite the key
        // ingested by background pulling. not visible.
        group_engine.tombstone(&mut wb, req.shard_id, &delete.key, super::FLAT_KEY_VERSION)?;
    } else {
        purge_versions(&mut wb, group_engine, req.shard_id, &delete.key).await?;
        group_engine.delete(&mut wb, req.shard_id, &delete.key, super::FLAT_KEY_VERSION)?;
    }
    Ok(EvalResult {
        batch: Some(WriteBatchRep {
            data: wb.data().to_owned(),
        }),
        ..Default::default()
    })
}

async fn purge_versions(
    wb: &mut WriteBatch,
    engine: &GroupEngine,
    shard_id: u64,
    key: &[u8],
) -> Result<()> {
    let snapshot_mode = SnapshotMode::Key { key };
    let mut snapshot = engine.snapshot(shard_id, snapshot_mode)?;
    if let Some(iter) = snapshot.mvcc_iter() {
        for entry in iter {
            engine.delete(wb, shard_id, key, entry.version())?;
        }
    }
    snapshot.status()?;
    Ok(())
}
