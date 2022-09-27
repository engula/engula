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

mod group;
mod state;

use std::{path::Path, sync::Arc};

pub use self::{
    group::{
        EngineConfig, GroupEngine, RawIterator, Snapshot, SnapshotMode, WriteBatch, WriteStates,
        LOCAL_COLLECTION_ID,
    },
    state::StateEngine,
};

type Result<T> = std::result::Result<T, rocksdb::Error>;

pub struct RawDb {
    pub options: rocksdb::Options,
    pub db: rocksdb::DB,
}

impl RawDb {
    #[inline]
    pub fn cf_handle(&self, name: &str) -> Option<Arc<rocksdb::BoundColumnFamily>> {
        self.db.cf_handle(name)
    }

    #[inline]
    pub fn create_cf<N: AsRef<str>>(&self, name: N) -> Result<()> {
        self.db.create_cf(name, &self.options)
    }

    #[inline]
    pub fn drop_cf(&self, name: &str) -> Result<()> {
        self.db.drop_cf(name)
    }

    #[inline]
    pub fn flush_cf(&self, cf: &impl rocksdb::AsColumnFamilyRef) -> Result<()> {
        self.db.flush_cf(cf)
    }

    #[inline]
    pub fn write_opt(
        &self,
        batch: rocksdb::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<()> {
        self.db.write_opt(batch, writeopts)
    }

    #[inline]
    pub fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>> {
        self.db.get_pinned_cf(cf, key)
    }

    #[inline]
    pub fn get_pinned_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
        readopts: &rocksdb::ReadOptions,
    ) -> Result<Option<rocksdb::DBPinnableSlice>> {
        self.db.get_pinned_cf_opt(cf, key, readopts)
    }

    #[inline]
    pub fn iterator_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        mode: rocksdb::IteratorMode,
    ) -> rocksdb::DBIteratorWithThreadMode<'b, rocksdb::DB> {
        self.db.iterator_cf(cf_handle, mode)
    }

    #[inline]
    pub fn iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
        mode: rocksdb::IteratorMode,
    ) -> rocksdb::DBIteratorWithThreadMode<'b, rocksdb::DB> {
        self.db.iterator_cf_opt(cf_handle, readopts, mode)
    }

    #[inline]
    pub fn ingest_external_file_opts<P: AsRef<Path>>(
        &self,
        opts: &rocksdb::IngestExternalFileOptions,
        paths: Vec<P>,
    ) -> Result<()> {
        self.db.ingest_external_file_opts(opts, paths)
    }

    #[inline]
    pub fn ingest_external_file_cf_opts<P: AsRef<Path>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        opts: &rocksdb::IngestExternalFileOptions,
        paths: Vec<P>,
    ) -> Result<()> {
        self.db.ingest_external_file_cf_opts(cf, opts, paths)
    }
}
