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

use std::time::Duration;

use engula_api::server::v1::{group_request_union::Request, *};

use super::{ExecCtx, Replica};
use crate::{
    node::{migrate::MigrateController, shard},
    Error, Result,
};

/// A wrapper function that detects and completes retries as quickly as possible.
pub async fn execute(
    replica: &Replica,
    exec_ctx: ExecCtx,
    request: GroupRequest,
) -> Result<GroupResponse> {
    execute_internal(None, replica, exec_ctx, request).await
}

pub async fn forwardable_execute(
    migrate_ctrl: &MigrateController,
    replica: &Replica,
    exec_ctx: ExecCtx,
    request: GroupRequest,
) -> Result<GroupResponse> {
    execute_internal(Some(migrate_ctrl), replica, exec_ctx, request).await
}

async fn execute_internal(
    migrate_ctrl: Option<&MigrateController>,
    replica: &Replica,
    mut exec_ctx: ExecCtx,
    request: GroupRequest,
) -> Result<GroupResponse> {
    exec_ctx.epoch = request.epoch;

    let request = request
        .request
        .as_ref()
        .and_then(|request| request.request.as_ref())
        .ok_or_else(|| Error::InvalidArgument("GroupRequest::request is None".into()))?;

    // TODO(walter) detect group request timeout.
    let mut freshed_descriptor = None;
    loop {
        match replica.execute(exec_ctx.clone(), request).await {
            Ok(resp) => {
                let resp = if let Some(descriptor) = freshed_descriptor {
                    GroupResponse::with_error(resp, Error::EpochNotMatch(descriptor).into())
                } else {
                    GroupResponse::new(resp)
                };
                return Ok(resp);
            }
            Err(Error::Forward(forward_ctx)) => {
                if let Some(ctrl) = migrate_ctrl {
                    let resp = ctrl.forward(forward_ctx, request).await?;
                    return Ok(GroupResponse::new(resp));
                } else {
                    panic!("receive forward response but no migration controller set");
                }
            }
            Err(Error::ServiceIsBusy(_)) | Err(Error::GroupNotReady(_)) => {
                // sleep and retry.
                crate::runtime::time::sleep(Duration::from_micros(200)).await;
            }
            Err(Error::EpochNotMatch(desc)) => {
                if is_executable(&desc, request) {
                    debug_assert_ne!(desc.epoch, exec_ctx.epoch);
                    exec_ctx.epoch = desc.epoch;
                    freshed_descriptor = Some(desc);
                    continue;
                }

                return Err(Error::EpochNotMatch(desc));
            }
            Err(Error::Raft(raft::Error::ProposalDropped)) => {
                return Err(Error::NotLeader(replica.info.group_id, None))
            }
            Err(e) => return Err(e),
        }
    }
}

fn is_executable(descriptor: &GroupDesc, request: &Request) -> bool {
    if !super::is_change_meta_request(request) {
        return match request {
            Request::Get(req) => {
                is_target_shard_exists(descriptor, req.shard_id, &req.get.as_ref().unwrap().key)
            }
            Request::Put(req) => {
                is_target_shard_exists(descriptor, req.shard_id, &req.put.as_ref().unwrap().key)
            }
            Request::Delete(req) => {
                is_target_shard_exists(descriptor, req.shard_id, &req.delete.as_ref().unwrap().key)
            }
            Request::PrefixList(req) => {
                is_target_shard_exists(descriptor, req.shard_id, &req.prefix)
            }
            Request::BatchWrite(req) => {
                for delete in &req.deletes {
                    if !is_target_shard_exists(
                        descriptor,
                        delete.shard_id,
                        &delete.delete.as_ref().unwrap().key,
                    ) {
                        return false;
                    }
                }
                for put in &req.puts {
                    if !is_target_shard_exists(
                        descriptor,
                        put.shard_id,
                        &put.put.as_ref().unwrap().key,
                    ) {
                        return false;
                    }
                }
                true
            }
            _ => unreachable!(),
        };
    }

    false
}

fn is_target_shard_exists(desc: &GroupDesc, shard_id: u64, key: &[u8]) -> bool {
    // TODO(walter) support migrate meta.
    desc.shards
        .iter()
        .find(|s| s.id == shard_id)
        .map(|s| shard::belong_to(s, key))
        .unwrap_or_default()
}
