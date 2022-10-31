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

use crate::{
    engine::GroupEngine,
    node::{migrate::ForwardCtx, replica::ExecCtx},
    Error, Result,
};

/// Get the value of the specified key.
pub(crate) async fn get(
    exec_ctx: &ExecCtx,
    engine: &GroupEngine,
    req: &ShardGetRequest,
) -> Result<Option<Vec<u8>>> {
    let get = req
        .get
        .as_ref()
        .ok_or_else(|| Error::InvalidArgument("ShardGetRequest::get is None".into()))?;

    let value = engine.get(req.shard_id, &get.key).await?;
    if let Some(desc) = exec_ctx.migration_desc.as_ref() {
        let shard_id = desc.shard_desc.as_ref().unwrap().id;
        if shard_id == req.shard_id {
            let payloads = if let Some(value) = value {
                vec![ShardData {
                    key: get.key.clone(),
                    value,
                    version: super::MIGRATING_KEY_VERSION,
                }]
            } else {
                Vec::default()
            };
            let forward_ctx = ForwardCtx {
                shard_id,
                dest_group_id: desc.dest_group_id,
                payloads,
            };
            return Err(Error::Forward(forward_ctx));
        }
    }
    Ok(value)
}
