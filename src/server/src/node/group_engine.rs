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

use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use engula_api::server::v1::GroupDesc;

use crate::Result;

/// The collection id of local states, which allows commit without replicating.
const LOCAL_COLLECTION_ID: u64 = 0;

pub type WriteBatch = rocksdb::WriteBatch;

/// A structure supports grouped data, metadata saving and retriving.
///
/// NOTE: Shard are managed by `GroupEngine` instead of a shard engine, because shards from
/// different collections in the same group needs to persist on disk at the same time, to guarantee
/// the accuracy of applied index.
#[allow(unused)]
pub struct GroupEngine
where
    Self: Send,
{
    name: String,
    raw_db: Arc<rocksdb::DB>,
    collections: Arc<RwLock<HashMap<u64, u64>>>,
}

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
    pub fn flushed_index(&self) -> u64 {
        todo!()
    }

    /// Get key value from the corresponding shard.
    pub async fn get(&self, shard_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");

        let collection_id = self
            .collection_id(shard_id)
            .expect("shard id to collection id");

        let raw_key = keys::raw(collection_id, key);
        let value = self.raw_db.get_cf(&cf_handle, raw_key)?;
        Ok(value)
    }

    /// Put key value into the corresponding shard.
    pub fn put(&self, wb: &mut WriteBatch, shard_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");

        let collection_id = self
            .collection_id(shard_id)
            .expect("shard id to collection id");

        let raw_key = keys::raw(collection_id, key);
        wb.put_cf(&cf_handle, raw_key, value);
        Ok(())
    }

    /// Delete key from the corresponding shard.
    pub fn delete(&self, wb: &mut WriteBatch, shard_id: u64, key: &[u8]) -> Result<()> {
        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");

        let collection_id = self
            .collection_id(shard_id)
            .expect("shard id to collection id");

        let raw_key = keys::raw(collection_id, key);
        wb.delete_cf(&cf_handle, raw_key);
        Ok(())
    }

    /// Set the applied index of this group.
    pub(super) fn set_applied_index(&self, wb: &mut WriteBatch, applied_index: u64) {
        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");

        let raw_key = keys::applied_index();
        wb.put_cf(&cf_handle, raw_key, applied_index.to_le_bytes().as_slice());
    }

    pub fn commit(&self, wb: WriteBatch) -> Result<()> {
        use rocksdb::WriteOptions;

        let mut opts = WriteOptions::default();
        opts.disable_wal(true);
        self.raw_db.write_opt(wb, &opts)?;
        Ok(())
    }

    // TODO(walter) support async iterator, may be a stream?
    pub async fn iter<'a>(&self) -> GroupEngineIterator<'a> {
        todo!()
    }

    /// Ingest data into group engine.
    pub async fn ingest(&self) -> Result<()> {
        todo!()
    }

    fn collection_id(&self, shard_id: u64) -> Option<u64> {
        self.collections
            .read()
            .expect("read lock")
            .get(&shard_id)
            .cloned()
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

#[allow(unused)]
mod keys {
    const APPLIED_INDEX: &[u8] = b"APPLIED_INDEX";

    pub fn raw(collection_id: u64, key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + key.len());
        buf.extend_from_slice(collection_id.to_le_bytes().as_slice());
        buf.extend_from_slice(key);
        buf
    }

    pub fn applied_index() -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + APPLIED_INDEX.len());
        buf.extend_from_slice(super::LOCAL_COLLECTION_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(APPLIED_INDEX);
        buf
    }
}
