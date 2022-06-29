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
use engula_api::server::v1::{shard_desc::*, *};

pub fn in_range(start: &[u8], end: &[u8], key: &[u8]) -> bool {
    start <= key && (key < end || end.is_empty())
}

/// Return whether a key belongs to the corresponding shard.
pub fn belong_to(shard: &ShardDesc, key: &[u8]) -> bool {
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(_hash) => {
            // TODO(walter) compute hash slot.
            false
        }
        Partition::Range(RangePartition { start, end }) => in_range(start, end, key),
    }
}

/// Return the start key of the corresponding shard.
#[inline]
pub fn start_key(shard: &ShardDesc) -> &[u8] {
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(_hash) => {
            todo!("the range of hash shard")
        }
        Partition::Range(RangePartition { start, .. }) => start.as_slice(),
    }
}

/// Return the end key of the corresponding shard.
#[inline]
pub fn end_key(shard: &ShardDesc) -> &[u8] {
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(_hash) => {
            todo!("the range of hash shard")
        }
        Partition::Range(RangePartition { end, .. }) => end.as_slice(),
    }
}
