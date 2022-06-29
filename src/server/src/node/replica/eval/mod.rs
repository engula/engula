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

mod cmd_accept_shard;
mod cmd_batch_write;
mod cmd_delete;
mod cmd_get;
mod cmd_prefix_list;
mod cmd_put;

use engula_api::server::v1::ShardDesc;

pub use self::{
    cmd_accept_shard::accept_shard, cmd_batch_write::batch_write, cmd_delete::delete, cmd_get::get,
    cmd_prefix_list::prefix_list, cmd_put::put,
};
use crate::serverpb::v1::EvalResult;

const FLAT_KEY_VERSION: u64 = u64::MAX - 1;
pub const MIGRATING_KEY_VERSION: u64 = 0;

pub fn add_shard(shard: ShardDesc) -> EvalResult {
    use crate::serverpb::v1::SyncOp;

    EvalResult {
        op: Some(SyncOp::add_shard(shard)),
        ..Default::default()
    }
}
