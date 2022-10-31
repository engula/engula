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

use engula_api::server::v1::ShardPutRequest;

use crate::{
    engine::{GroupEngine, WriteBatch},
    node::{migrate::ForwardCtx, replica::ExecCtx},
    serverpb::v1::{EvalResult, WriteBatchRep},
    Error, Result,
};

pub(crate) async fn put(
    exec_ctx: &ExecCtx,
    group_engine: &GroupEngine,
    req: &ShardPutRequest,
) -> Result<EvalResult> {
    let put = req
        .put
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("ShardPutRequest::put is None".into()))?;

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
    group_engine.put(
        &mut wb,
        req.shard_id,
        &put.key,
        &put.value,
        super::FLAT_KEY_VERSION,
    )?;
    Ok(EvalResult {
        batch: Some(WriteBatchRep {
            data: wb.data().to_owned(),
        }),
        ..Default::default()
    })
}
