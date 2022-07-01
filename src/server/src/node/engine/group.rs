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
    cell::RefCell,
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Arc, RwLock},
};

use engula_api::server::v1::*;
use prost::Message;
use tracing::info;

use crate::{bootstrap::INITIAL_EPOCH, node::shard, serverpb::v1::*, Error, Result};

/// The collection id of local states, which allows commit without replicating.
pub const LOCAL_COLLECTION_ID: u64 = 0;

#[derive(Default)]
pub struct WriteBatch {
    apply_state: Option<ApplyState>,
    descriptor: Option<GroupDesc>,
    migration_state: Option<MigrationState>,
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
    core: Arc<RwLock<GroupEngineCore>>,
}

#[derive(Default)]
struct GroupEngineCore {
    group_desc: GroupDesc,
    shard_descs: HashMap<u64, ShardDesc>,
    migration_state: Option<MigrationState>,
}

/// Traverse the data of the group engine, but don't care about the data format.
pub struct RawIterator<'a> {
    apply_state: ApplyState,
    descriptor: GroupDesc,
    db_iter: rocksdb::DBIterator<'a>,
}

enum SnapshotRange {
    Target { target_key: Vec<u8> },
    Prefix { prefix: Vec<u8> },
    Range { start: Vec<u8>, end: Vec<u8> },
}

pub struct Snapshot<'a> {
    collection_id: u64,
    range: Option<SnapshotRange>,

    core: RefCell<SnapshotCore<'a>>,
}

pub struct SnapshotCore<'a> {
    db_iter: rocksdb::DBIterator<'a>,
    current_key: Option<Vec<u8>>,
    cached_entry: Option<MvccEntry>,
}

/// Traverse the data of a shard in the group engine, analyze and return the data (including
/// tombstone).
pub struct UserDataIterator<'a, 'b> {
    snapshot: &'b Snapshot<'a>,
}

/// Traverse multi-version of a single key.
pub struct MvccIterator<'a, 'b> {
    snapshot: &'b Snapshot<'a>,
}

pub struct MvccEntry {
    key: Box<[u8]>,
    user_key: Vec<u8>,
    value: Box<[u8]>,
}

#[derive(Debug)]
pub enum SnapshotMode<'a> {
    Start { start_key: Option<&'a [u8]> },
    Key { key: &'a [u8] },
    Prefix { key: &'a [u8] },
}

struct ColumnFamilyDecorator<'a, 'b> {
    cf_handle: Arc<rocksdb::BoundColumnFamily<'b>>,
    wb: &'a mut rocksdb::WriteBatch,
}

impl GroupEngine {
    /// Create a new instance of group engine.
    pub async fn create(raw_db: Arc<rocksdb::DB>, group_desc: &GroupDesc) -> Result<()> {
        use rocksdb::Options;

        let group_id = group_desc.id;
        let name = group_id.to_string();
        info!("create group engine for {}, cf name is {}", group_id, name);
        // FIXME(walter) clean staled data if the column families already exists.
        debug_assert!(raw_db.cf_handle(&name).is_none());
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
            core: Arc::new(RwLock::new(GroupEngineCore {
                group_desc: desc.clone(),
                shard_descs: Default::default(),
                migration_state: None,
            })),
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

        let group_desc = internal::descriptor(&raw_db, &cf_handle)?;
        let migration_state = internal::migration_state(&raw_db, &cf_handle)?;
        let mut shard_descs = internal::shard_descs(&group_desc);
        if let Some(shard_desc) = migration_state.as_ref().map(|m| m.get_shard_desc()) {
            shard_descs
                .entry(shard_desc.id)
                .or_insert_with(|| shard_desc.clone());
        }
        let core = GroupEngineCore {
            migration_state,
            group_desc,
            shard_descs,
        };

        // Flush mem tables so that subsequent `ReadTier::Persisted` can be executed.
        raw_db.flush_cf(&cf_handle)?;
        Ok(Some(GroupEngine {
            name,
            raw_db: raw_db.clone(),
            core: Arc::new(RwLock::new(core)),
        }))
    }

    /// Destory a group engine.
    pub async fn destory(group_id: u64, raw_db: Arc<rocksdb::DB>) -> Result<()> {
        let name = group_id.to_string();
        raw_db.drop_cf(&name)?;
        info!("destory column family {}", name);
        Ok(())
    }

    /// Return the migrate state.
    #[inline]
    pub fn migration_state(&self) -> Option<MigrationState> {
        self.core.read().unwrap().migration_state.clone()
    }

    /// Return the group descriptor.
    #[inline]
    pub fn descriptor(&self) -> GroupDesc {
        self.core.read().unwrap().group_desc.clone()
    }

    /// Return the persisted apply state of raft.
    #[inline]
    pub fn flushed_apply_state(&self) -> Result<ApplyState> {
        internal::flushed_apply_state(&self.raw_db, &self.cf_handle())
    }

    /// Get key value from the corresponding shard.
    pub async fn get(&self, shard_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot_mode = SnapshotMode::Key { key };
        let mut snapshot = self.snapshot(shard_id, snapshot_mode)?;
        if let Some(mut iter) = snapshot.mvcc_iter() {
            if let Some(entry) = iter.next() {
                return Ok(entry.value().map(ToOwned::to_owned));
            }
        }
        snapshot.status()?;
        Ok(None)
    }

    /// Put key value into the corresponding shard.
    pub fn put(
        &self,
        wb: &mut WriteBatch,
        shard_id: u64,
        key: &[u8],
        value: &[u8],
        version: u64,
    ) -> Result<()> {
        let desc = self.shard_desc(shard_id)?;
        let collection_id = desc.collection_id;
        debug_assert_ne!(collection_id, LOCAL_COLLECTION_ID);
        debug_assert!(shard::belong_to(&desc, key));

        wb.put(
            keys::mvcc_key(collection_id, key, version),
            values::data(value),
        );

        Ok(())
    }

    /// Logically delete key from the corresponding shard.
    pub fn tombstone(
        &self,
        wb: &mut WriteBatch,
        shard_id: u64,
        key: &[u8],
        version: u64,
    ) -> Result<()> {
        let desc = self.shard_desc(shard_id)?;
        let collection_id = desc.collection_id;
        debug_assert_ne!(collection_id, LOCAL_COLLECTION_ID);
        debug_assert!(shard::belong_to(&desc, key));

        wb.put(
            keys::mvcc_key(collection_id, key, version),
            values::tombstone(),
        );

        Ok(())
    }

    pub fn delete(
        &self,
        wb: &mut WriteBatch,
        shard_id: u64,
        key: &[u8],
        version: u64,
    ) -> Result<()> {
        let desc = self.shard_desc(shard_id)?;
        let collection_id = desc.collection_id;
        debug_assert_ne!(collection_id, LOCAL_COLLECTION_ID);
        debug_assert!(shard::belong_to(&desc, key));

        wb.delete(keys::mvcc_key(collection_id, key, version));

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

    #[inline]
    pub fn set_migration_state(&self, wb: &mut WriteBatch, migration_state: &MigrationState) {
        wb.migration_state = Some(migration_state.clone());
    }

    pub fn commit(&self, mut wb: WriteBatch, persisted: bool) -> Result<()> {
        use rocksdb::WriteOptions;

        let cf_handle = self.cf_handle();
        let mut inner_wb = rocksdb::WriteBatch::default();
        let mut decorator = ColumnFamilyDecorator {
            cf_handle: cf_handle.clone(),
            wb: &mut inner_wb,
        };
        wb.inner.iterate(&mut decorator);
        wb.inner = inner_wb;
        wb.write_states(&self.cf_handle());

        let mut opts = WriteOptions::default();
        if persisted {
            opts.set_sync(true);
        } else {
            opts.disable_wal(true);
        }
        self.raw_db.write_opt(wb.inner, &opts)?;

        if wb.descriptor.is_some() || wb.migration_state.is_some() {
            self.apply_core_states(wb.descriptor, wb.migration_state);
        }

        Ok(())
    }

    pub fn snapshot(&self, shard_id: u64, mode: SnapshotMode) -> Result<Snapshot> {
        use rocksdb::{Direction, IteratorMode, ReadOptions};

        let desc = self.shard_desc(shard_id)?;
        let collection_id = desc.collection_id;
        debug_assert_ne!(collection_id, LOCAL_COLLECTION_ID);

        let opts = ReadOptions::default();
        let key = match &mode {
            SnapshotMode::Start { start_key } => {
                let start_key = start_key.unwrap_or_else(|| shard::start_key(&desc));
                debug_assert!(shard::belong_to(&desc, start_key));
                keys::raw(collection_id, start_key)
            }
            SnapshotMode::Key { key } => {
                debug_assert!(shard::belong_to(&desc, key));
                keys::raw(collection_id, key)
            }
            SnapshotMode::Prefix { key } => {
                debug_assert!(shard::belong_to(&desc, key));
                keys::raw(collection_id, key)
            }
        };
        let inner_mode = IteratorMode::From(&key, Direction::Forward);
        let iter = self
            .raw_db
            .iterator_cf_opt(&self.cf_handle(), opts, inner_mode);
        Ok(Snapshot::new(collection_id, iter, mode, &desc))
    }

    pub fn raw_iter(&self) -> Result<RawIterator> {
        use rocksdb::{IteratorMode, ReadOptions};

        let opts = ReadOptions::default();
        let iter = self
            .raw_db
            .iterator_cf_opt(&self.cf_handle(), opts, IteratorMode::Start);
        RawIterator::new(iter)
    }

    /// Ingest data into group engine.
    pub fn ingest<P: AsRef<Path>>(&self, files: Vec<P>) -> Result<()> {
        use rocksdb::{IngestExternalFileOptions, Options};

        self.raw_db.drop_cf(&self.name)?;
        self.raw_db.create_cf(&self.name, &Options::default())?;

        let opts = IngestExternalFileOptions::default();
        let cf_handle = self.cf_handle();
        self.raw_db
            .ingest_external_file_cf_opts(&cf_handle, &opts, files)?;

        let group_desc = internal::descriptor(&self.raw_db, &cf_handle)?;
        let migration_state = internal::migration_state(&self.raw_db, &cf_handle)?;
        self.apply_core_states(Some(group_desc), migration_state);

        Ok(())
    }

    pub fn apply_core_states(
        &self,
        descriptor: Option<GroupDesc>,
        migration_state: Option<MigrationState>,
    ) {
        let mut core = self.core.write().unwrap();
        if let Some(desc) = descriptor {
            core.group_desc = desc;
        }

        // TODO(walter) remove shard desc if migration task is aborted.
        if let Some(migration_state) = migration_state {
            if migration_state.step == MigrationStep::Finished as i32
                || migration_state.step == MigrationStep::Aborted as i32
            {
                core.migration_state = None;
            } else {
                core.migration_state = Some(migration_state);
            }
        }

        core.shard_descs = internal::shard_descs(&core.group_desc);
        if let Some(shard_desc) = core
            .migration_state
            .as_ref()
            .map(|m| m.get_shard_desc().clone())
        {
            core.shard_descs.entry(shard_desc.id).or_insert(shard_desc);
        }
    }

    #[inline]
    fn shard_desc(&self, shard_id: u64) -> Result<ShardDesc> {
        self.core
            .read()
            .expect("read lock")
            .shard_descs
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| Error::InvalidArgument(format!("no such {} shard exists", shard_id)))
    }

    #[inline]
    fn cf_handle(&self) -> Arc<rocksdb::BoundColumnFamily> {
        self.raw_db
            .cf_handle(&self.name)
            .expect("column family handle")
    }
}

impl<'a> RawIterator<'a> {
    fn new(mut db_iter: rocksdb::DBIterator<'a>) -> Result<Self> {
        use rocksdb::IteratorMode;

        let apply_state = next_message(&mut db_iter, &keys::apply_state())?;
        let descriptor = next_message(&mut db_iter, &keys::descriptor())?;
        db_iter.set_mode(IteratorMode::Start);

        Ok(RawIterator {
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

impl<'a> Iterator for RawIterator<'a> {
    /// Key value pairs.
    type Item = <rocksdb::DBIterator<'a> as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.db_iter.next()
    }
}

impl<'a> Snapshot<'a> {
    fn new<'b>(
        collection_id: u64,
        db_iter: rocksdb::DBIterator<'a>,
        snapshot_mode: SnapshotMode<'b>,
        desc: &ShardDesc,
    ) -> Self {
        let range = match snapshot_mode {
            SnapshotMode::Key { key } => Some(SnapshotRange::Target {
                target_key: key.to_owned(),
            }),
            SnapshotMode::Prefix { key } => Some(SnapshotRange::Prefix {
                prefix: key.to_owned(),
            }),
            SnapshotMode::Start { start_key } => Some(SnapshotRange::Range {
                start: start_key
                    .unwrap_or_else(|| shard::start_key(desc))
                    .to_owned(),
                end: shard::end_key(desc).to_owned(),
            }),
        };

        Snapshot {
            collection_id,
            range,
            core: RefCell::new(SnapshotCore {
                db_iter,
                current_key: None,
                cached_entry: None,
            }),
        }
    }

    #[inline]
    pub fn iter<'b>(&'b mut self) -> UserDataIterator<'a, 'b> {
        UserDataIterator { snapshot: self }
    }

    #[inline]
    pub fn mvcc_iter<'b>(&'b mut self) -> Option<MvccIterator<'a, 'b>> {
        self.next_mvcc_iterator()
    }

    #[inline]
    pub fn status(&self) -> Result<()> {
        self.core.borrow().db_iter.status()?;
        Ok(())
    }

    fn next_mvcc_iterator<'b>(&'b self) -> Option<MvccIterator<'a, 'b>> {
        let mut core = self.core.borrow_mut();
        loop {
            if let Some(entry) = core.cached_entry.as_ref() {
                if let Some(range) = self.range.as_ref() {
                    if !range.is_valid_key(entry.user_key()) {
                        // The iterate target has been consumed.
                        return None;
                    }
                }

                // Skip iterated keys.
                // TODO(walter) support seek to next user key to skip old versions.
                if !core.is_current_key(entry.user_key()) {
                    core.current_key = Some(entry.user_key().to_owned());
                    return Some(MvccIterator { snapshot: self });
                }
            }

            core.next_entry(self.collection_id)?;
        }
    }

    fn next_mvcc_entry(&self) -> Option<MvccEntry> {
        let mut core = self.core.borrow_mut();
        loop {
            if let Some(entry) = core.cached_entry.take() {
                if core.is_current_key(entry.user_key()) {
                    return Some(entry);
                } else {
                    core.cached_entry = Some(entry);
                    return None;
                }
            }

            core.next_entry(self.collection_id)?;
        }
    }
}

impl<'a> SnapshotCore<'a> {
    fn next_entry(&mut self, collection_id: u64) -> Option<()> {
        let (key, value) = match self.db_iter.next() {
            Some(v) => v,
            None => return None,
        };

        let prefix = &key[..core::mem::size_of::<u64>()];
        if prefix != collection_id.to_le_bytes().as_slice() {
            return None;
        }

        self.cached_entry = Some(MvccEntry::new(key, value));
        Some(())
    }

    #[inline]
    fn is_current_key(&self, target_key: &[u8]) -> bool {
        self.current_key
            .as_ref()
            .map(|k| k == target_key)
            .unwrap_or_default()
    }
}

impl<'a, 'b> Iterator for UserDataIterator<'a, 'b> {
    type Item = MvccIterator<'a, 'b>;

    fn next(&mut self) -> Option<MvccIterator<'a, 'b>> {
        self.snapshot.next_mvcc_iterator()
    }
}

impl<'a, 'b> Iterator for MvccIterator<'a, 'b> {
    type Item = MvccEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.snapshot.next_mvcc_entry()
    }
}

impl MvccEntry {
    fn new(key: Box<[u8]>, value: Box<[u8]>) -> Self {
        let user_key = keys::revert_mvcc_key(&key);
        MvccEntry {
            key,
            user_key,
            value,
        }
    }

    #[inline]
    pub fn raw_key(&self) -> &[u8] {
        &self.key
    }

    #[inline]
    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    pub fn version(&self) -> u64 {
        const L: usize = core::mem::size_of::<u64>();
        let len = self.key.len();
        let bytes = &self.key[(len - L)..];
        let mut buf = [0u8; L];
        buf[..].copy_from_slice(bytes);
        !u64::from_be_bytes(buf)
    }

    /// Return value of this `MvccEntry`. `None` is returned if this entry is a tombstone.
    pub fn value(&self) -> Option<&[u8]> {
        if self.value[0] == values::TOMBSTONE {
            None
        } else {
            debug_assert_eq!(self.value[0], values::DATA);
            Some(&self.value[1..])
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value[0] == values::TOMBSTONE
    }

    pub fn is_data(&self) -> bool {
        self.value[0] == values::DATA
    }
}

impl SnapshotRange {
    #[inline]
    fn is_valid_key(&self, key: &[u8]) -> bool {
        match self {
            SnapshotRange::Target { target_key } if target_key == key => true,
            SnapshotRange::Prefix { prefix } if key.starts_with(prefix) => true,
            SnapshotRange::Range { start, end } if shard::in_range(start, end, key) => true,
            _ => false,
        }
    }
}

impl<'a> Default for SnapshotMode<'a> {
    fn default() -> Self {
        SnapshotMode::Start { start_key: None }
    }
}

mod keys {
    const APPLY_STATE: &[u8] = b"APPLY_STATE";
    const DESCRIPTOR: &[u8] = b"DESCRIPTOR";
    const MIGRATE_STATE: &[u8] = b"MIGRATE_STATE";

    #[inline]
    pub fn raw(collection_id: u64, key: &[u8]) -> Vec<u8> {
        if key.is_empty() {
            collection_id.to_le_bytes().as_slice().to_owned()
        } else {
            mvcc_key(collection_id, key, u64::MAX)
        }
    }

    /// Generate mvcc key with the memcomparable format.
    pub fn mvcc_key(collection_id: u64, key: &[u8], version: u64) -> Vec<u8> {
        use std::io::{Cursor, Read};

        debug_assert!(!key.is_empty());
        let actual_len = (((key.len() - 1) / 8) + 1) * 9;
        let mut buf = Vec::with_capacity(2 * core::mem::size_of::<u64>() + actual_len);
        buf.extend_from_slice(collection_id.to_le_bytes().as_slice());
        let mut cursor = Cursor::new(key);
        while !cursor.is_empty() {
            let mut group = [0u8; 8];
            let mut size = cursor.read(&mut group[..]).unwrap() as u8;
            debug_assert_ne!(size, 0);
            if size == 8 && !cursor.is_empty() {
                size += 1;
            }
            buf.extend_from_slice(group.as_slice());
            buf.push(b'0' + size);
        }
        buf.extend_from_slice((!version).to_be_bytes().as_slice());
        buf
    }

    pub fn revert_mvcc_key(key: &[u8]) -> Vec<u8> {
        use std::io::{Cursor, Read};

        const L: usize = core::mem::size_of::<u64>();
        let len = key.len();
        debug_assert!(len > 2 * L);
        let encoded_user_key = &key[L..(len - L)];
        debug_assert_eq!(encoded_user_key.len() % 9, 0);
        let num_groups = encoded_user_key.len() / 9;
        let mut buf = Vec::with_capacity(num_groups * 8);
        let mut cursor = Cursor::new(encoded_user_key);
        while !cursor.is_empty() {
            let mut group = [0u8; 9];
            let _ = cursor.read(&mut group[..]).unwrap();
            let num_element = std::cmp::min((group[8] - b'0') as usize, 8);
            buf.extend_from_slice(&group[..num_element]);
        }
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

    #[inline]
    pub fn migrate_state() -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + MIGRATE_STATE.len());
        buf.extend_from_slice(super::LOCAL_COLLECTION_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(MIGRATE_STATE);
        buf
    }
}

mod values {
    pub(super) const DATA: u8 = 0;
    pub(super) const TOMBSTONE: u8 = 1;

    #[inline]
    pub fn tombstone() -> &'static [u8] {
        &[TOMBSTONE]
    }

    pub fn data(v: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(v.len() + 1);
        buf.push(DATA);
        buf.extend_from_slice(v);
        buf
    }
}

impl<'a, 'b> rocksdb::WriteBatchIterator for ColumnFamilyDecorator<'a, 'b> {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.wb.put_cf(&self.cf_handle, key, value);
    }

    fn delete(&mut self, key: Box<[u8]>) {
        self.wb.delete_cf(&self.cf_handle, key);
    }
}

impl WriteBatch {
    pub fn new(content: &[u8]) -> Self {
        WriteBatch {
            inner: rocksdb::WriteBatch::new(content),
            ..Default::default()
        }
    }

    fn write_states(&mut self, cf_handle: &impl rocksdb::AsColumnFamilyRef) {
        let inner_wb = &mut self.inner;
        if let Some(apply_state) = &self.apply_state {
            inner_wb.put_cf(cf_handle, keys::apply_state(), apply_state.encode_to_vec());
        }
        if let Some(desc) = &self.descriptor {
            inner_wb.put_cf(cf_handle, keys::descriptor(), desc.encode_to_vec());
        }
        if let Some(migration_state) = &self.migration_state {
            // Migrations in abort or finish steps are not persisted.
            if migration_state.step != MigrationStep::Finished as i32
                && migration_state.step != MigrationStep::Aborted as i32
            {
                inner_wb.put_cf(
                    cf_handle,
                    keys::migrate_state(),
                    migration_state.encode_to_vec(),
                );
            } else {
                inner_wb.delete_cf(cf_handle, keys::migrate_state());
            }
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

mod internal {
    use prost::Message;

    use super::*;

    pub(super) fn descriptor(
        db: &rocksdb::DB,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
    ) -> Result<GroupDesc> {
        let value = db
            .get_pinned_cf(cf_handle, keys::descriptor())?
            .expect("group descriptor will persisted when creating group");
        Ok(GroupDesc::decode(value.as_ref())?)
    }

    pub(super) fn migration_state(
        db: &rocksdb::DB,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
    ) -> Result<Option<MigrationState>> {
        if let Some(v) = db.get_pinned_cf(cf_handle, keys::migrate_state())? {
            Ok(Some(MigrationState::decode(v.as_ref())?))
        } else {
            Ok(None)
        }
    }

    pub(super) fn flushed_apply_state(
        db: &rocksdb::DB,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
    ) -> Result<ApplyState> {
        use rocksdb::{ReadOptions, ReadTier};
        let mut opt = ReadOptions::default();
        opt.set_read_tier(ReadTier::Persisted);
        let value = db
            .get_pinned_cf_opt(cf_handle, keys::apply_state(), &opt)?
            .expect("apply state will persisted when creating group");
        Ok(ApplyState::decode(value.as_ref())?)
    }

    #[inline]
    pub(super) fn shard_descs(group_desc: &GroupDesc) -> HashMap<u64, ShardDesc> {
        group_desc
            .shards
            .iter()
            .map(|shard| (shard.id, shard.clone()))
            .collect::<HashMap<_, _>>()
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

#[cfg(test)]
mod tests {
    use engula_api::server::v1::ShardDesc;
    use tempdir::TempDir;

    use super::*;
    use crate::runtime::{Executor, ExecutorOwner};

    #[test]
    fn memory_comparable_format() {
        struct Less {
            left: &'static [u8],
            left_version: u64,
            right: &'static [u8],
            right_version: u64,
        }

        let tests = vec![
            // 1. compare version
            Less {
                left: b"1",
                left_version: 1,
                right: b"1",
                right_version: 0,
            },
            Less {
                left: b"1",
                left_version: 256,
                right: b"1",
                right_version: 255,
            },
            Less {
                left: b"12345678",
                left_version: 256,
                right: b"12345678",
                right_version: 255,
            },
            Less {
                left: b"123456789",
                left_version: 256,
                right: b"123456789",
                right_version: 255,
            },
            // 2. different length
            Less {
                left: b"12345678",
                left_version: u64::MAX,
                right: b"123456789",
                right_version: 0,
            },
            Less {
                left: b"12345678",
                left_version: u64::MAX,
                right: b"12345678\x00",
                right_version: 0,
            },
            Less {
                left: b"12345678",
                left_version: u64::MAX,
                right: b"12345678\x00\x00\x00\x00\x00\x00\x00\x00",
                right_version: 0,
            },
            Less {
                left: b"12345678\x00\x00\x00",
                left_version: 0,
                right: b"12345678\x00\x00\x00\x00",
                right_version: 0,
            },
        ];
        for (idx, t) in tests.iter().enumerate() {
            let left = keys::mvcc_key(0, t.left, t.left_version);
            let right = keys::mvcc_key(0, t.right, t.right_version);
            assert!(
                left < right,
                "index {}, left {:?}, right {:?}",
                idx,
                left,
                right
            );
        }
    }

    fn create_engine(executor: Executor, group_id: u64, shard_id: u64) -> GroupEngine {
        create_engine_with_range(executor, group_id, shard_id, vec![], vec![])
    }

    fn create_engine_with_range(
        executor: Executor,
        group_id: u64,
        shard_id: u64,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> GroupEngine {
        use shard_desc::*;

        let tmp_dir = TempDir::new("engula").unwrap().into_path();
        let db_dir = tmp_dir.join("db");

        use crate::bootstrap::open_engine;

        let db = open_engine(db_dir).unwrap();
        let db = Arc::new(db);
        let group_engine = executor.block_on(async move {
            let desc = GroupDesc {
                id: group_id,
                ..Default::default()
            };
            GroupEngine::create(db.clone(), &desc).await.unwrap();
            GroupEngine::open(group_id, db).await.unwrap().unwrap()
        });

        let mut wb = WriteBatch::default();
        group_engine.set_group_desc(
            &mut wb,
            &GroupDesc {
                id: group_id,
                shards: vec![ShardDesc {
                    id: shard_id,
                    collection_id: 1,
                    partition: Some(Partition::Range(RangePartition { start, end })),
                }],
                ..Default::default()
            },
        );
        group_engine.commit(wb, false).unwrap();

        group_engine
    }

    #[test]
    fn mvcc_iterator() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload {
                key: b"123456",
                version: 1,
            },
            Payload {
                key: b"123456",
                version: 5,
            },
            Payload {
                key: b"123456",
                version: 256,
            },
            Payload {
                key: b"123456789",
                version: 0,
            },
        ];

        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let group_engine = create_engine(executor, 1, 1);
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine
                .put(&mut wb, 1, payload.key, b"", payload.version)
                .unwrap();
        }
        group_engine.commit(wb, false).unwrap();

        let mut snapshot = group_engine.snapshot(1, SnapshotMode::default()).unwrap();
        let mut user_data_iter = snapshot.iter();
        {
            // key 123456
            let mut mvcc_iter = user_data_iter.next().unwrap();
            let entry = mvcc_iter.next().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 256);

            let entry = mvcc_iter.next().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 5);

            let entry = mvcc_iter.next().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 1);

            assert!(mvcc_iter.next().is_none());
        }

        {
            // key 123456789
            let mut mvcc_iter = user_data_iter.next().unwrap();
            let entry = mvcc_iter.next().unwrap();
            assert_eq!(entry.user_key(), b"123456789");
            assert_eq!(entry.version(), 0);

            assert!(mvcc_iter.next().is_none());
        }
        assert!(snapshot.status().is_ok());
    }

    #[test]
    fn user_key_iterator() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload {
                key: b"123456",
                version: 1,
            },
            Payload {
                key: b"123456",
                version: 5,
            },
            Payload {
                key: b"123456",
                version: 256,
            },
            Payload {
                key: b"123456789",
                version: 0,
            },
        ];

        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let group_engine = create_engine(executor, 1, 1);
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine
                .put(&mut wb, 1, payload.key, b"", payload.version)
                .unwrap();
        }
        group_engine.commit(wb, false).unwrap();

        let mut snapshot = group_engine.snapshot(1, SnapshotMode::default()).unwrap();
        let mut user_data_iter = snapshot.iter();
        {
            // key 123456
            let mut mvcc_iter = user_data_iter.next().unwrap();
            let entry = mvcc_iter.next().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 256);
        }

        {
            // key 123456789, user_data_iter should skip the iterated keys.
            let mut mvcc_iter = user_data_iter.next().unwrap();
            let entry = mvcc_iter.next().unwrap();
            assert_eq!(entry.user_key(), b"123456789");
            assert_eq!(entry.version(), 0);

            assert!(mvcc_iter.next().is_none());
        }

        assert!(snapshot.status().is_ok());
    }

    #[test]
    fn iterate_target_key() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload {
                key: b"123456",
                version: 1,
            },
            Payload {
                key: b"123456",
                version: 5,
            },
            Payload {
                key: b"123456",
                version: 256,
            },
            Payload {
                key: b"123456789",
                version: 0,
            },
        ];

        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let group_engine = create_engine(executor, 1, 1);
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine
                .put(&mut wb, 1, payload.key, b"", payload.version)
                .unwrap();
        }
        group_engine.commit(wb, false).unwrap();

        {
            // Target key `123456`
            let snapshot_mode = SnapshotMode::Key { key: b"123456" };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            let mut user_data_iter = snapshot.iter();
            assert!(user_data_iter.next().is_some());
            assert!(user_data_iter.next().is_none());
            assert!(snapshot.status().is_ok());
        }

        {
            // Target key `123456789`
            let snapshot_mode = SnapshotMode::Key { key: b"123456789" };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            let mut user_data_iter = snapshot.iter();
            assert!(user_data_iter.next().is_some());
            assert!(user_data_iter.next().is_none());
            assert!(snapshot.status().is_ok());
        }

        {
            // Target to an not existed key
            let snapshot_mode = SnapshotMode::Key { key: b"???" };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            let mut user_data_iter = snapshot.iter();
            assert!(user_data_iter.next().is_none());
            assert!(snapshot.status().is_ok());
        }
    }

    #[test]
    fn get_latest_version() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let group_engine = create_engine(executor.clone(), 1, 1);
        let mut wb = WriteBatch::default();
        group_engine
            .put(&mut wb, 1, b"a12345678", b"", 123)
            .unwrap();
        group_engine
            .tombstone(&mut wb, 1, b"a12345678", 124)
            .unwrap();
        group_engine
            .put(&mut wb, 1, b"b12345678", b"123", 123)
            .unwrap();
        group_engine
            .put(&mut wb, 1, b"b12345678", b"124", 124)
            .unwrap();
        group_engine.commit(wb, false).unwrap();

        executor.block_on(async move {
            let v = group_engine.get(1, b"a12345678").await.unwrap();
            assert!(v.is_none());

            let v = group_engine.get(1, b"b12345678").await.unwrap();
            assert!(v.is_some());
        });
    }

    #[test]
    fn iterate_in_range() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let group_engine = create_engine(executor, 1, 1);
        let mut wb = WriteBatch::default();
        group_engine.put(&mut wb, 1, b"a", b"", 123).unwrap();
        group_engine.tombstone(&mut wb, 1, b"a", 124).unwrap();
        group_engine.put(&mut wb, 1, b"b", b"123", 123).unwrap();
        group_engine.put(&mut wb, 1, b"b", b"124", 124).unwrap();
        group_engine.commit(wb, false).unwrap();

        // Add new shard
        use shard_desc::*;
        let mut wb = WriteBatch::default();
        group_engine.set_group_desc(
            &mut wb,
            &GroupDesc {
                id: 1,
                shards: vec![
                    ShardDesc {
                        id: 1,
                        collection_id: 1,
                        partition: Some(Partition::Range(RangePartition {
                            start: vec![],
                            end: b"b".to_vec(),
                        })),
                    },
                    ShardDesc {
                        id: 2,
                        collection_id: 1,
                        partition: Some(Partition::Range(RangePartition {
                            start: b"b".to_vec(),
                            end: vec![],
                        })),
                    },
                ],
                ..Default::default()
            },
        );
        group_engine.commit(wb, false).unwrap();

        // Iterate shard 1
        let snapshot_mode = SnapshotMode::default();
        let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
        let mut user_data_iter = snapshot.iter();
        let mut mvcc_key_iter = user_data_iter.next().unwrap();
        let entry = mvcc_key_iter.next().unwrap();
        assert_eq!(entry.user_key(), b"a");
        assert!(user_data_iter.next().is_none());

        // Iterate shard 2
        let snapshot_mode = SnapshotMode::default();
        let mut snapshot = group_engine.snapshot(2, snapshot_mode).unwrap();
        let mut user_data_iter = snapshot.iter();
        let mut mvcc_key_iter = user_data_iter.next().unwrap();
        let entry = mvcc_key_iter.next().unwrap();
        assert_eq!(entry.user_key(), b"b");
        assert!(user_data_iter.next().is_none());

        assert!(snapshot.status().is_ok());
    }

    #[test]
    fn cf_id_irrelevant_write_batch() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let engine_1 = create_engine(executor.clone(), 1, 1);
        let engine_2 = create_engine(executor.clone(), 1, 1);

        // Put in engine 1, commit in engine 2.
        let mut wb = WriteBatch::default();
        engine_1.put(&mut wb, 1, b"a", b"", 123).unwrap();
        engine_1.put(&mut wb, 1, b"b", b"123", 123).unwrap();

        engine_2.commit(wb, false).unwrap();
    }
}
