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

pub mod apply;
pub mod create;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub use create::dispatch_creating_snap_task;

use crate::{serverpb::v1::SnapshotMeta, Result};

const SNAP_DATA: &str = "DATA";
const SNAP_TEMP: &str = "TEMP";
const SNAP_META: &str = "META";

pub struct SnapshotInfo {
    pub meta: SnapshotMeta,
    pub base_dir: PathBuf,
}

#[allow(unused)]
pub struct SnapshotGuard {
    info: SnapshotInfo,
    manager: SnapManager,
}

#[allow(unused)]
pub struct ReplicaSnapManager {
    base_dir: PathBuf,
    snapshots: Vec<SnapshotInfo>,
}

#[allow(unused)]
#[derive(Clone)]
pub struct SnapManager
where
    Self: Send + Sync,
{
    replicas: Arc<Mutex<HashMap<u64, ReplicaSnapManager>>>,
}

#[allow(unused)]
impl SnapManager {
    pub fn recovery<P: AsRef<Path>>(base_dir: P) -> Result<SnapManager> {
        // TODO(walter) recovery snap manager from disk.
        Ok(SnapManager {
            replicas: Arc::default(),
        })
    }

    /// Mark group as creating, and return a dir to save snapshot.
    pub fn create(&self, replica_id: u64) -> PathBuf {
        todo!();
    }

    pub fn install(&self, replica_id: u64, dir_name: &Path, meta: &SnapshotMeta) {
        todo!()
    }

    pub fn latest_snap(&self, replica_id: u64) -> Option<SnapshotInfo> {
        todo!();
    }

    pub fn lock_snap(&self, replica_id: u64, id: &[u8]) -> Option<SnapshotGuard> {
        todo!()
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        todo!("release snapshot info")
    }
}

impl std::ops::Deref for SnapshotGuard {
    type Target = SnapshotInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}
