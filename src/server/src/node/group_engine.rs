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

use std::marker::PhantomData;

use engula_api::server::v1::GroupDesc;

use crate::Result;

/// A structure supports grouped data, metadata saving and retriving.
///
/// NOTE: Shard are managed by `GroupEngine` instead of a shard engine, because shards from
/// different collections in the same group needs to persist on disk at the same time, to guarantee
/// the accuracy of applied index.
#[allow(unused)]
pub struct GroupEngine
where
    Self: Send, {}

#[allow(unused)]
pub struct GroupEngineIterator<'a> {
    mark: PhantomData<&'a ()>,
}

#[allow(unused)]
impl GroupEngine {
    /// Return the group descriptor
    pub fn descriptor(&self) -> GroupDesc {
        todo!()
    }

    /// Return the persisted applied index of raft.
    pub fn applied_index(&self) -> u64 {
        todo!()
    }

    /// Get key value from corresponding shard.
    pub async fn get(&self, shard_id: u64, key: &[u8]) -> Result<Option<Box<[u8]>>> {
        todo!()
    }

    /// Put key value into corresponding shard.
    pub async fn put(&self, shard_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    // TODO(walter) support async iterator, may be a stream?
    pub async fn iter<'a>(&self) -> GroupEngineIterator<'a> {
        todo!()
    }

    /// Ingest data into group engine.
    pub async fn ingest(&self) -> Result<()> {
        todo!()
    }
}

#[allow(unused)]
impl<'a> GroupEngineIterator<'a> {
    pub fn applied_index(&self) -> u64 {
        todo!()
    }

    pub fn descriptor(&self) -> GroupDesc {
        todo!()
    }
}

impl<'a> Iterator for GroupEngineIterator<'a> {
    /// Key value pairs.
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
