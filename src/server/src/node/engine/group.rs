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
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Arc, RwLock},
};

use engula_api::server::v1::GroupDesc;
use prost::Message;

use crate::{bootstrap::INITIAL_EPOCH, serverpb::v1::ApplyState, Error, Result};

/// The collection id of local states, which allows commit without replicating.
pub const LOCAL_COLLECTION_ID: u64 = 0;

#[derive(Default)]
pub struct WriteBatch {
    apply_state: Option<ApplyState>,
    descriptor: Option<GroupDesc>,
    inner: rocksdb::WriteBatch,
}

/// A structure supports grouped data, metadata saving and retriving.
///
/// NOTE: Shard are managed by `GroupEngine` instead of a shard engine, because shards from
/// different collections in the same group needs to persist on disk at the same time, to guarantee
/// the accuracy of applied index.
#[derive(Clone)]
pub struct GroupEngine
where
    Self: Send,
{
    name: String,
    raw_db: Arc<rocksdb::DB>,
    collections: Arc<RwLock<HashMap<u64, u64>>>,
}

pub struct GroupEngineIterator<'a> {
    apply_state: ApplyState,
    descriptor: GroupDesc,
    db_iter: rocksdb::DBIterator<'a>,
}

impl GroupEngine {
    /// Create a new instance of group engine.
    pub async fn create(raw_db: Arc<rocksdb::DB>, group_desc: &GroupDesc) -> Result<()> {
        use rocksdb::Options;

        let group_id = group_desc.id;
        let name = group_id.to_string();
        // FIXME(walter) clean staled data if the column families already exists.
        raw_db.create_cf(&name, &Options::default())?;

        let desc = GroupDesc {
            id: group_id,
            epoch: INITIAL_EPOCH,
            shards: vec![],
            replicas: vec![],
        };

        let engine = GroupEngine {
            name,
            raw_db,
            collections: Arc::default(),
        };

        // The group descriptor should be persisted into disk.
        let mut wb = WriteBatch::default();
        engine.set_apply_state(&mut wb, 0, 0);
        engine.set_group_desc(&mut wb, &desc);
        engine.commit(wb, true)?;

        Ok(())
    }

    /// Open the exists instance of group engine.
    pub async fn open(group_id: u64, raw_db: Arc<rocksdb::DB>) -> Result<Option<Self>> {
        let name = group_id.to_string();
        let cf_handle = match raw_db.cf_handle(&name) {
            Some(cf_handle) => cf_handle,
            None => {
                return Ok(None);
            }
        };
        let raw_key = keys::descriptor();
        let value = raw_db
            .get_pinned_cf(&cf_handle, raw_key)?
            .expect("group descriptor will persisted when creating group");
        let desc = GroupDesc::decode(value.as_ref()).expect("group descriptor format");
        let collections = desc
            .shards
            .iter()
            .map(|shard| (shard.id, shard.parent_id))
            .collect::<HashMap<_, _>>();
        Ok(Some(GroupEngine {
            name,
            raw_db: raw_db.clone(),
            collections: Arc::new(RwLock::new(collections)),
        }))
    }

    /// Destory a group engine.
    pub async fn destory(group_id: u64, raw_db: Arc<rocksdb::DB>) -> Result<()> {
        let name = group_id.to_string();
        raw_db.drop_cf(&name)?;
        Ok(())
    }

    /// Return the group descriptor
    pub fn descriptor(&self) -> Result<GroupDesc> {
        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");
        let raw_key = keys::descriptor();
        let value = self
            .raw_db
            .get_pinned_cf(&cf_handle, raw_key)?
            .expect("group descriptor will persisted when creating group");
        let desc = GroupDesc::decode(value.as_ref()).expect("group descriptor format");
        Ok(desc)
    }

    /// Return the persisted apply state of raft.
    pub fn flushed_apply_state(&self) -> ApplyState {
        use rocksdb::{ReadOptions, ReadTier};
        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");
        let mut opt = ReadOptions::default();
        opt.set_read_tier(ReadTier::Persisted);
        let raw_key = keys::apply_state();
        let value = self
            .raw_db
            .get_pinned_cf_opt(&cf_handle, raw_key, &opt)
            .unwrap()
            .expect("group descriptor will persisted when creating group");
        ApplyState::decode(value.as_ref()).expect("should encoded ApplyState")
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

    /// Set the applied state of this group.
    #[inline]
    pub fn set_apply_state(&self, wb: &mut WriteBatch, index: u64, term: u64) {
        wb.apply_state = Some(ApplyState { index, term });
    }

    #[inline]
    pub fn set_group_desc(&self, wb: &mut WriteBatch, desc: &GroupDesc) {
        wb.descriptor = Some(desc.clone());
    }

    pub fn commit(&self, wb: WriteBatch, persisted: bool) -> Result<()> {
        use rocksdb::WriteOptions;

        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");

        let mut inner_wb = wb.inner;
        if let Some(apply_state) = wb.apply_state {
            inner_wb.put_cf(&cf_handle, keys::apply_state(), apply_state.encode_to_vec());
        }
        if let Some(desc) = &wb.descriptor {
            inner_wb.put_cf(&cf_handle, keys::descriptor(), desc.encode_to_vec());
        }

        let mut opts = WriteOptions::default();
        if persisted {
            opts.set_sync(true);
        } else {
            opts.disable_wal(true);
        }
        self.raw_db.write_opt(inner_wb, &opts)?;

        if let Some(desc) = wb.descriptor {
            let mut collections = self.collections.write().unwrap();
            collections.clear();
            for shard in desc.shards {
                collections.insert(shard.id, shard.parent_id);
            }
        }

        Ok(())
    }

    pub fn iter(&self) -> Result<GroupEngineIterator> {
        use rocksdb::{IteratorMode, ReadOptions};

        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");
        let opts = ReadOptions::default();
        let iter = self
            .raw_db
            .iterator_cf_opt(&cf_handle, opts, IteratorMode::Start);
        GroupEngineIterator::new(iter)
    }

    /// Ingest data into group engine.
    pub fn ingest<P: AsRef<Path>>(&self, files: Vec<P>) -> Result<()> {
        let cf_handle = self
            .raw_db
            .cf_handle(&self.name)
            .expect("column family handle");

        use rocksdb::{IngestExternalFileOptions, Options};
        self.raw_db.drop_cf(&self.name)?;
        self.raw_db.create_cf(&self.name, &Options::default())?;

        let opts = IngestExternalFileOptions::default();
        self.raw_db
            .ingest_external_file_cf_opts(&cf_handle, &opts, files)?;
        Ok(())
    }

    fn collection_id(&self, shard_id: u64) -> Option<u64> {
        self.collections
            .read()
            .expect("read lock")
            .get(&shard_id)
            .cloned()
    }
}

impl<'a> GroupEngineIterator<'a> {
    fn new(mut db_iter: rocksdb::DBIterator<'a>) -> Result<Self> {
        use rocksdb::IteratorMode;

        let apply_state = next_message(&mut db_iter, &keys::apply_state())?;
        let descriptor = next_message(&mut db_iter, &keys::descriptor())?;
        db_iter.set_mode(IteratorMode::Start);

        Ok(GroupEngineIterator {
            apply_state,
            descriptor,
            db_iter,
        })
    }

    #[inline]
    pub fn apply_state(&self) -> &ApplyState {
        &self.apply_state
    }

    #[inline]
    pub fn descriptor(&self) -> &GroupDesc {
        &self.descriptor
    }

    #[inline]
    pub fn status(&mut self) -> Result<()> {
        db_iterator_status(&mut self.db_iter, true)
    }
}

impl<'a> Iterator for GroupEngineIterator<'a> {
    /// Key value pairs.
    type Item = <rocksdb::DBIterator<'a> as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.db_iter.next()
    }
}

mod keys {
    const APPLY_STATE: &[u8] = b"APPLY_STATE";
    const DESCRIPTOR: &[u8] = b"DESCRIPTOR";

    #[inline]
    pub fn raw(collection_id: u64, key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + key.len());
        buf.extend_from_slice(collection_id.to_le_bytes().as_slice());
        buf.extend_from_slice(key);
        buf
    }

    #[inline]
    pub fn apply_state() -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + APPLY_STATE.len());
        buf.extend_from_slice(super::LOCAL_COLLECTION_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(APPLY_STATE);
        buf
    }

    #[inline]
    pub fn descriptor() -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + DESCRIPTOR.len());
        buf.extend_from_slice(super::LOCAL_COLLECTION_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(DESCRIPTOR);
        buf
    }
}

impl WriteBatch {
    pub fn new(content: &[u8]) -> Self {
        WriteBatch {
            inner: rocksdb::WriteBatch::new(content),
            ..Default::default()
        }
    }
}

impl Deref for WriteBatch {
    type Target = rocksdb::WriteBatch;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for WriteBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

fn db_iterator_status(db_iter: &mut rocksdb::DBIterator<'_>, allow_not_found: bool) -> Result<()> {
    debug_assert!(!db_iter.valid());
    match db_iter.status() {
        Ok(()) if allow_not_found => Ok(()),
        Ok(()) => Err(Error::InvalidData("no such key exists".into())),
        Err(err) => Err(err.into()),
    }
}

fn next_message<T: prost::Message + Default>(
    db_iter: &mut rocksdb::DBIterator<'_>,
    key: &[u8],
) -> Result<T> {
    use rocksdb::{Direction, IteratorMode};

    db_iter.set_mode(IteratorMode::From(key, Direction::Forward));
    if let Some((_, value)) = db_iter.next() {
        Ok(T::decode(&*value).expect("should encoded with T"))
    } else {
        db_iterator_status(db_iter, false)?;
        unreachable!()
    }
}