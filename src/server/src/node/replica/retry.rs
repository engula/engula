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

use engula_api::server::v1::{
    group_request_union::Request, GroupDesc, GroupRequest, GroupResponse, ShardDesc,
};

use super::Replica;
use crate::{Error, Result};

/// A wrapper function that detects and completes retries as quickly as possible.
pub async fn execute(replica: &Replica, mut request: GroupRequest) -> Result<GroupResponse> {
    // TODO(walter) detect group request timeout.
    let mut freshed_descriptor = None;
    loop {
        match replica.execute(&request).await {
            Ok(mut resp) => {
                if let Some(descriptor) = freshed_descriptor {
                    resp.error = Some(Error::EpochNotMatch(descriptor).into());
                }
                return Ok(resp);
            }
            Err(Error::Forward(forward_ctx)) => {
                let ctrl = replica.migrate_ctrl();
                return ctrl.forward(forward_ctx, &request).await;
            }
            Err(Error::ServiceIsBusy(_)) | Err(Error::GroupNotReady(_)) => {
                // sleep and retry.
                crate::runtime::time::sleep(Duration::from_micros(200)).await;
            }
            Err(Error::EpochNotMatch(desc)) => {
                if is_executable(&desc, &request) {
                    request.epoch = desc.epoch;
                    freshed_descriptor = Some(desc);
                    continue;
                }

                return Err(Error::EpochNotMatch(desc));
            }
            Err(e) => return Err(e),
        }
    }
}

fn is_executable(descriptor: &GroupDesc, request: &GroupRequest) -> bool {
    let request = request
        .request
        .as_ref()
        .and_then(|request| request.request.as_ref())
        .unwrap();
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
    desc.shards
        .iter()
        .find(|s| s.id == shard_id)
        .map(|s| is_key_located_in_shard(s, key))
        .unwrap_or_default()
}

fn is_key_located_in_shard(shard: &ShardDesc, key: &[u8]) -> bool {
    use engula_api::server::v1::shard_desc::Partition;
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(_hash) => {
            // TODO(walter) compute hash slot.
            false
        }
        Partition::Range(range) => {
            range.start.as_slice() <= key && (key < range.end.as_slice() || range.end.is_empty())
        }
    }
}
