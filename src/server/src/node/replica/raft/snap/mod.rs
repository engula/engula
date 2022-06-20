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
pub mod download;
pub mod send;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub use self::{create::dispatch_creating_snap_task, download::dispatch_downloading_snap_task};
use crate::{serverpb::v1::SnapshotMeta, Result};

const SNAP_DATA: &str = "DATA";
const SNAP_TEMP: &str = "TEMP";
const SNAP_META: &str = "META";

/// A wrapper of snapshot meta, with the parent dir.
#[derive(Clone)]
pub struct SnapshotInfo {
    pub snapshot_id: Vec<u8>,
    /// The parent dir of snapshot files.
    pub base_dir: PathBuf,
    pub meta: SnapshotMeta,

    /// The ref count of snapshot.
    ref_count: usize,
}

pub struct SnapshotGuard {
    replica_id: u64,
    info: SnapshotInfo,
    manager: SnapManager,
}

#[allow(unused)]
pub struct ReplicaSnapManager {
    replica_id: u64,
    /// The parent dir of an replica snapshots.
    base_dir: PathBuf,
    next_snapshot_index: usize,
    snapshots: Vec<SnapshotInfo>,
}

#[allow(unused)]
#[derive(Clone)]
pub struct SnapManager
where
    Self: Send + Sync,
{
    shared: Arc<SnapManagerShared>,
}

struct SnapManagerShared {
    root_dir: PathBuf,
    replicas: Mutex<HashMap<u64, ReplicaSnapManager>>,
}

#[allow(unused)]
impl SnapManager {
    pub fn recovery<P: AsRef<Path>>(root_dir: P) -> Result<SnapManager> {
        // TODO(walter) recovery snap manager from disk.
        Ok(SnapManager {
            shared: Arc::new(SnapManagerShared {
                root_dir: root_dir.as_ref().to_owned(),
                replicas: Mutex::default(),
            }),
        })
    }

    /// Mark group as creating, and return a dir to save snapshot.
    pub fn create(&self, replica_id: u64) -> PathBuf {
        let mut replicas = self.shared.replicas.lock().unwrap();
        replicas
            .entry(replica_id)
            .or_insert_with(|| ReplicaSnapManager::new(replica_id, self.shared.root_dir.clone()))
            .next_snapshot_dir()
    }

    /// Install a snapshot and returns snapshot id.
    pub fn install(&self, replica_id: u64, dir_name: &Path, meta: &SnapshotMeta) -> Vec<u8> {
        // TODO(walter) check snapshot data integrity.
        let mut replicas = self.shared.replicas.lock().unwrap();
        let replica = replicas
            .get_mut(&replica_id)
            .expect("replica should exists during download/create snapshot");
        let parent = dir_name.parent();
        match parent {
            Some(parent) if parent == replica.base_dir => {
                use std::os::unix::ffi::OsStrExt;
                let name = dir_name.file_name().unwrap().to_string_lossy().to_owned();
                let snapshot_index = name.parse::<usize>().expect("install invalid snapshot dir");
                let snapshot_id = format!("{}", snapshot_index).as_bytes().to_owned();
                debug_assert!(snapshot_index < replica.next_snapshot_index);
                replica.snapshots.push(SnapshotInfo {
                    snapshot_id: snapshot_id.clone(),
                    base_dir: replica.base_dir.join(name.as_ref()),
                    meta: meta.clone(),
                    ref_count: 0,
                });
                snapshot_id
            }
            _ => panic!("install invalid snapshot dir"),
        }
    }

    pub fn latest_snap(&self, replica_id: u64) -> Option<SnapshotInfo> {
        let mut replicas = self.shared.replicas.lock().unwrap();
        replicas
            .get(&replica_id)
            .and_then(|rep| rep.snapshots.last())
            .cloned()
    }

    pub fn lock_snap(&self, replica_id: u64, snapshot_id: &[u8]) -> Option<SnapshotGuard> {
        let mut replicas = self.shared.replicas.lock().unwrap();
        replicas
            .get_mut(&replica_id)
            .and_then(|rep| rep.snapshot(snapshot_id))
            .map(|info| SnapshotGuard {
                info,
                replica_id,
                manager: self.clone(),
            })
    }
}

impl ReplicaSnapManager {
    fn new(replica_id: u64, root_dir: PathBuf) -> Self {
        let base_dir = root_dir.join(&format!("{}", replica_id));
        ReplicaSnapManager {
            replica_id,
            base_dir,
            next_snapshot_index: 0,
            snapshots: vec![],
        }
    }

    fn next_snapshot_dir(&mut self) -> PathBuf {
        let snapshot_index = self.next_snapshot_index;
        self.next_snapshot_index += 1;
        self.base_dir.join(format!("{}", snapshot_index))
    }

    fn snapshot(&mut self, snapshot_id: &[u8]) -> Option<SnapshotInfo> {
        for snapshot in &mut self.snapshots {
            if snapshot.snapshot_id == snapshot_id {
                snapshot.ref_count += 1;
                return Some(snapshot.clone());
            }
        }
        None
    }

    fn release(&mut self, snapshot_id: &[u8]) {
        for snapshot in &mut self.snapshots {
            if snapshot.snapshot_id == snapshot_id {
                snapshot.ref_count -= 1;
                break;
            }
        }
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        let mut replicas = self.manager.shared.replicas.lock().unwrap();
        if let Some(rep) = replicas.get_mut(&self.replica_id) {
            rep.release(&self.info.snapshot_id)
        }
    }
}

impl std::ops::Deref for SnapshotGuard {
    type Target = SnapshotInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}
