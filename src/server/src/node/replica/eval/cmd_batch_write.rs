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
use engula_api::server::v1::BatchWriteRequest;

use crate::{
    node::{engine::WriteBatch, replica::ExecCtx, GroupEngine},
    serverpb::v1::{EvalResult, WriteBatchRep},
    Error, Result,
};

pub async fn batch_write(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &BatchWriteRequest,
) -> Result<Option<EvalResult>> {
    if req.deletes.is_empty() && req.puts.is_empty() {
        return Ok(None);
    }

    let mut wb = WriteBatch::default();
    for req in &req.deletes {
        let del = req
            .delete
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("ShardDeleteRequest::delete is None".into()))?;
        if exec_ctx.is_migrating_shard(req.shard_id) {
            panic!("BatchWrite does not support migrating shard");
        }
        group_engine.delete(&mut wb, req.shard_id, &del.key)?;
    }
    for req in &req.puts {
        let put = req
            .put
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("ShardPutRequest::put is None".into()))?;
        if exec_ctx.is_migrating_shard(req.shard_id) {
            panic!("BatchWrite does not support migrating shard");
        }
        group_engine.put(&mut wb, req.shard_id, &put.key, &put.value)?;
    }
    Ok(Some(EvalResult {
        batch: Some(WriteBatchRep {
            data: wb.data().to_owned(),
        }),
        ..Default::default()
    }))
}
