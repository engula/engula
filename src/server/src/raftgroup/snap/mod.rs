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
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::{channel::mpsc, StreamExt};
use raft::prelude::{Snapshot, SnapshotMetadata};
use tracing::{error, info};

pub use self::{create::dispatch_creating_snap_task, download::dispatch_downloading_snap_task};
use crate::{
    runtime::{Executor, TaskPriority},
    serverpb::v1::SnapshotMeta,
    Result,
};

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
    created_at: Instant,
}

pub struct SnapshotGuard {
    replica_id: u64,
    info: SnapshotInfo,
    manager: SnapManager,
}

pub struct ReplicaSnapManager {
    /// The parent dir of an replica snapshots.
    base_dir: PathBuf,
    next_snapshot_index: usize,
    snapshots: Vec<SnapshotInfo>,
}

#[derive(Clone)]
pub struct SnapManager
where
    Self: Send + Sync,
{
    shared: Arc<SnapManagerShared>,
}

struct SnapManagerShared {
    root_dir: PathBuf,
    inner: Mutex<SnapManagerInner>,
}

struct SnapManagerInner {
    sender: mpsc::UnboundedSender<(u64, PathBuf)>,
    replicas: HashMap<u64, ReplicaSnapManager>,
}

impl SnapManager {
    #[cfg(test)]
    pub fn new(dir: PathBuf) -> SnapManager {
        let (sender, _) = mpsc::unbounded();
        SnapManager {
            shared: Arc::new(SnapManagerShared {
                root_dir: dir,
                inner: Mutex::new(SnapManagerInner {
                    sender,
                    replicas: HashMap::default(),
                }),
            }),
        }
    }

    pub fn recovery<P: AsRef<Path>>(executor: &Executor, root_dir: P) -> Result<SnapManager> {
        use prost::Message;

        let (mut sender, receiver) = mpsc::unbounded();
        executor.spawn(None, TaskPriority::IoLow, async move {
            recycle_snapshot(receiver).await;
        });

        let root_dir = root_dir.as_ref();
        let mut replicas = HashMap::new();
        for (replica_id, replica_dir) in list_numeric_path(root_dir)? {
            for (index, snap_dir) in list_numeric_path(&replica_dir)? {
                let meta_name = snap_dir.join(SNAP_DATA);
                if !std::fs::try_exists(&meta_name)? {
                    sender
                        .start_send((replica_id, snap_dir))
                        .unwrap_or_default();
                    continue;
                }
                let bytes = std::fs::read(&meta_name)?;
                let snapshot_meta = match SnapshotMeta::decode(&*bytes) {
                    Ok(meta) => meta,
                    Err(_) => {
                        sender
                            .start_send((replica_id, snap_dir))
                            .unwrap_or_default();
                        continue;
                    }
                };

                let snapshot_id = format!("{}", index).as_bytes().to_owned();
                let info = SnapshotInfo {
                    snapshot_id,
                    base_dir: snap_dir,
                    meta: snapshot_meta,
                    ref_count: 0,
                    created_at: Instant::now(),
                };
                let replica_mgr = replicas
                    .entry(replica_id)
                    .or_insert_with(|| ReplicaSnapManager::new(replica_id, replica_dir.clone()));
                replica_mgr.next_snapshot_index =
                    std::cmp::max(replica_mgr.next_snapshot_index, index as usize);
                replica_mgr.snapshots.push(info);
            }
        }

        Ok(SnapManager {
            shared: Arc::new(SnapManagerShared {
                root_dir: root_dir.to_owned(),
                inner: Mutex::new(SnapManagerInner { sender, replicas }),
            }),
        })
    }

    /// Mark group as creating, and return a dir to save snapshot.
    pub fn create(&self, replica_id: u64) -> PathBuf {
        let mut inner = self.shared.inner.lock().unwrap();
        inner
            .replicas
            .entry(replica_id)
            .or_insert_with(|| ReplicaSnapManager::new(replica_id, self.shared.root_dir.clone()))
            .next_snapshot_dir()
    }

    /// Install a snapshot and returns snapshot id.
    pub fn install(&self, replica_id: u64, dir_name: &Path, meta: &SnapshotMeta) -> Vec<u8> {
        // TODO(walter) check snapshot data integrity.
        let mut inner = self.shared.inner.lock().unwrap();
        let replica = inner
            .replicas
            .get_mut(&replica_id)
            .expect("replica should exists during download/create snapshot");
        let parent = dir_name.parent();
        match parent {
            Some(parent) if parent == replica.base_dir => {
                let name = dir_name.file_name().unwrap().to_string_lossy().to_owned();
                let snapshot_index = name.parse::<usize>().expect("install invalid snapshot dir");
                let snapshot_id = format!("{}", snapshot_index).as_bytes().to_owned();
                debug_assert!(snapshot_index < replica.next_snapshot_index);
                replica.snapshots.push(SnapshotInfo {
                    snapshot_id: snapshot_id.clone(),
                    base_dir: replica.base_dir.join(name.as_ref()),
                    meta: meta.clone(),
                    ref_count: 0,
                    created_at: Instant::now(),
                });
                snapshot_id
            }
            _ => panic!("install invalid snapshot dir"),
        }
    }

    pub fn latest_snap(&self, replica_id: u64) -> Option<SnapshotInfo> {
        let inner = self.shared.inner.lock().unwrap();
        inner
            .replicas
            .get(&replica_id)
            .and_then(|rep| rep.snapshots.last())
            .cloned()
    }

    pub fn lock_snap(&self, replica_id: u64, snapshot_id: &[u8]) -> Option<SnapshotGuard> {
        let mut inner = self.shared.inner.lock().unwrap();
        inner
            .replicas
            .get_mut(&replica_id)
            .and_then(|rep| rep.snapshot(snapshot_id))
            .map(|info| SnapshotGuard {
                info,
                replica_id,
                manager: self.clone(),
            })
    }

    pub fn recycle_snapshots(&self, replica_id: u64, required_index: u64) {
        let now = Instant::now();
        let (mut sender, snapshots) = {
            let mut inner = self.shared.inner.lock().unwrap();
            let replica = inner.replicas.get_mut(&replica_id);
            if replica.is_none() {
                return;
            }

            let replica = replica.unwrap();
            let snapshots = replica
                .snapshots
                .drain_filter(|info| {
                    info.meta.apply_state.as_ref().unwrap().index < required_index
                        && info.created_at + Duration::from_secs(300) < now
                })
                .map(|info| info.base_dir)
                .collect::<Vec<_>>();
            if replica.snapshots.is_empty() {
                inner.replicas.remove(&replica_id);
            }
            (inner.sender.clone(), snapshots)
        };

        for snap_dir in snapshots {
            sender
                .start_send((replica_id, snap_dir))
                .unwrap_or_default();
        }
    }
}

impl ReplicaSnapManager {
    fn new(replica_id: u64, root_dir: PathBuf) -> Self {
        let base_dir = root_dir.join(&format!("{}", replica_id));
        ReplicaSnapManager {
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

impl SnapshotInfo {
    pub fn to_raft_snapshot(&self) -> Snapshot {
        let snap_meta = &self.meta;
        let apply_state = snap_meta.apply_state.clone().unwrap();
        let conf_state =
            super::conf_state_from_group_descriptor(snap_meta.group_desc.as_ref().unwrap());
        let raft_meta = SnapshotMetadata {
            conf_state: Some(conf_state),
            index: apply_state.index,
            term: apply_state.term,
        };
        Snapshot {
            data: self.snapshot_id.clone(),
            metadata: Some(raft_meta),
        }
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        let mut inner = self.manager.shared.inner.lock().unwrap();
        if let Some(rep) = inner.replicas.get_mut(&self.replica_id) {
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

fn list_numeric_path(root: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut values = vec![];
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(OsStr::to_str) {
            let index: u64 = match name.parse() {
                Ok(id) => id,
                Err(_) => continue,
            };
            values.push((index, path));
        }
    }
    Ok(values)
}

async fn recycle_snapshot(mut receiver: mpsc::UnboundedReceiver<(u64, PathBuf)>) {
    while let Some((replica_id, snapshot_dir)) = receiver.next().await {
        if let Err(err) = std::fs::remove_dir_all(&snapshot_dir) {
            error!(
                "replica {} recycle snapshot {}: {}",
                replica_id,
                snapshot_dir.display(),
                err
            );
            continue;
        }

        info!(
            "replica {} recycle snapshot {}",
            replica_id,
            snapshot_dir.display()
        );
        // Remove parent directory if it is empty.
        if let Some(parent) = snapshot_dir.parent() {
            std::fs::remove_dir(parent).unwrap_or_default();
        }
    }
}
