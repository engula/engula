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

use crate::serverpb::v1::*;

pub async fn accept_shard(group_id: u64, epoch: u64, req: &AcceptShardRequest) -> EvalResult {
    let migration_desc = MigrationDesc {
        shard_desc: req.shard_desc.clone(),
        src_group_id: req.src_group_id,
        src_group_epoch: req.src_group_epoch,
        dest_group_id: group_id,
        dest_group_epoch: epoch,
    };
    let migration = Migration {
        event: migration::Event::Prepare as i32,
        migration_desc: Some(migration_desc),
        ..Default::default()
    };
    let sync_op = SyncOp {
        migration: Some(migration),
        ..Default::default()
    };
    EvalResult {
        batch: None,
        op: Some(sync_op),
    }
}
