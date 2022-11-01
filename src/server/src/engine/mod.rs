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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use tracing::info;

pub(crate) use self::{
    group::{GroupEngine, RawIterator, SnapshotMode, WriteBatch, WriteStates},
    state::StateEngine,
};
use crate::{DbConfig, Result};

// The disk layouts.
const LAYOUT_DATA: &str = "db";
const LAYOUT_LOG: &str = "log";
const LAYOUT_SNAP: &str = "snap";

type DbResult<T> = Result<T, rocksdb::Error>;

pub(crate) struct RawDb {
    pub options: rocksdb::Options,
    pub db: rocksdb::DB,
}

impl RawDb {
    #[inline]
    pub fn cf_handle(&self, name: &str) -> Option<Arc<rocksdb::BoundColumnFamily>> {
        self.db.cf_handle(name)
    }

    #[inline]
    pub fn create_cf<N: AsRef<str>>(&self, name: N) -> DbResult<()> {
        self.db.create_cf(name, &self.options)
    }

    #[inline]
    pub fn drop_cf(&self, name: &str) -> DbResult<()> {
        self.db.drop_cf(name)
    }

    #[inline]
    pub fn flush_cf(&self, cf: &impl rocksdb::AsColumnFamilyRef) -> DbResult<()> {
        self.db.flush_cf(cf)
    }

    #[inline]
    pub fn write_opt(
        &self,
        batch: rocksdb::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> DbResult<()> {
        self.db.write_opt(batch, writeopts)
    }

    #[inline]
    pub fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> DbResult<Option<rocksdb::DBPinnableSlice>> {
        self.db.get_pinned_cf(cf, key)
    }

    #[inline]
    pub fn get_pinned_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
        readopts: &rocksdb::ReadOptions,
    ) -> DbResult<Option<rocksdb::DBPinnableSlice>> {
        self.db.get_pinned_cf_opt(cf, key, readopts)
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
    pub fn ingest_external_file_cf_opts<P: AsRef<Path>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        opts: &rocksdb::IngestExternalFileOptions,
        paths: Vec<P>,
    ) -> DbResult<()> {
        self.db.ingest_external_file_cf_opts(cf, opts, paths)
    }
}

#[derive(Clone)]
pub(crate) struct Engines {
    log_path: PathBuf,
    _db_path: PathBuf,
    log: Arc<raft_engine::Engine>,
    db: Arc<RawDb>,
    state: StateEngine,
}

impl Engines {
    pub(crate) fn open(root_dir: &Path, db_cfg: &DbConfig) -> Result<Self> {
        let db_path = root_dir.join(LAYOUT_DATA);
        let log_path = root_dir.join(LAYOUT_LOG);
        let db = Arc::new(open_engine(db_cfg, &db_path)?);
        let log = Arc::new(open_raft_engine(&log_path)?);
        let state = StateEngine::new(log.clone());
        Ok(Engines {
            log_path,
            _db_path: db_path,
            log,
            db,
            state,
        })
    }

    #[inline]
    pub(crate) fn log(&self) -> Arc<raft_engine::Engine> {
        self.log.clone()
    }

    #[inline]
    pub(crate) fn db(&self) -> Arc<RawDb> {
        self.db.clone()
    }

    #[inline]
    pub(crate) fn state(&self) -> StateEngine {
        self.state.clone()
    }

    #[inline]
    pub(crate) fn snap_dir(&self) -> PathBuf {
        self.log_path.join(LAYOUT_SNAP)
    }
}

pub(crate) fn open_engine<P: AsRef<Path>>(cfg: &DbConfig, path: P) -> Result<RawDb> {
    use rocksdb::DB;

    std::fs::create_dir_all(&path)?;
    let options = cfg.to_options();

    // List column families and open database with column families.
    match DB::list_cf(&options, &path) {
        Ok(cfs) => {
            info!("open local db with {} column families", cfs.len());
            let db = DB::open_cf_with_opts(
                &options,
                path,
                cfs.into_iter().map(|name| (name, options.clone())),
            )?;
            Ok(RawDb { db, options })
        }
        Err(e) => {
            if e.as_ref().ends_with("CURRENT: No such file or directory") {
                info!("create new local db");
                let db = DB::open(&options, &path)?;
                Ok(RawDb { db, options })
            } else {
                Err(e.into())
            }
        }
    }
}

fn open_raft_engine(log_path: &Path) -> Result<raft_engine::Engine> {
    use raft_engine::{Config, Engine};
    let engine_dir = log_path.join("engine");
    let snap_dir = log_path.join("snap");
    create_dir_all_if_not_exists(&engine_dir)?;
    create_dir_all_if_not_exists(&snap_dir)?;
    let engine_cfg = Config {
        dir: engine_dir.to_str().unwrap().to_owned(),
        enable_log_recycle: false,
        ..Default::default()
    };
    Ok(Engine::open(engine_cfg)?)
}

fn create_dir_all_if_not_exists<P: AsRef<Path>>(dir: &P) -> Result<()> {
    use std::io::ErrorKind;
    match std::fs::create_dir_all(dir.as_ref()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err.into()),
    }
}
