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

#[inline]
pub fn key_slot(key: &[u8], slots: u32) -> u32 {
    // TODO: it's temp hash impl..
    crc32fast::hash(key) % slots
}

/// Return whether a key belongs to the corresponding shard.
pub fn belong_to(shard: &ShardDesc, key: &[u8]) -> bool {
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(hash) => hash.slot_id == key_slot(key, hash.slots),
        Partition::Range(RangePartition { start, end }) => in_range(start, end, key),
    }
}

/// Return the start key of the corresponding shard.
#[inline]
pub fn start_key(shard: &ShardDesc) -> Vec<u8> {
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(hash) => hash.slot_id.to_le_bytes().as_slice().to_owned(),
        Partition::Range(RangePartition { start, .. }) => start.as_slice().to_owned(),
    }
}

/// Return the end key of the corresponding shard.
#[inline]
pub fn end_key(shard: &ShardDesc) -> Vec<u8> {
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(hash) => (hash.slot_id + 1).to_le_bytes().as_slice().to_owned(),
        Partition::Range(RangePartition { end, .. }) => end.as_slice().to_owned(),
    }
}

/// Return the slot of the corresponding shard.  `None` is returned if shard is range partition.
pub fn slot(shard: &ShardDesc) -> Option<u32> {
    match shard.partition.as_ref().unwrap() {
        Partition::Hash(hash) => Some(hash.slot_id),
        Partition::Range(_) => None,
    }
}
