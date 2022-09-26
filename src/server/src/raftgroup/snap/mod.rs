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
use tracing::{error, info, warn};

pub use self::{create::dispatch_creating_snap_task, download::dispatch_downloading_snap_task};
use crate::{runtime::TaskPriority, serverpb::v1::SnapshotMeta, Result};

const SNAP_DATA: &str = "DATA";
const SNAP_TEMP: &str = "TEMP";
const SNAP_META: &str = "META";

#[derive(Debug)]
pub enum RecycleSnapMode {
    RequiredIndex(u64),
    All,
}

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
    min_keep_intervals: Duration,
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
                min_keep_intervals: Duration::from_secs(0),
                inner: Mutex::new(SnapManagerInner {
                    sender,
                    replicas: HashMap::default(),
                }),
            }),
        }
    }

    pub async fn recovery<P: AsRef<Path>>(root_dir: P) -> Result<SnapManager> {
        use prost::Message;

        let (mut sender, receiver) = mpsc::unbounded();
        crate::runtime::current().spawn(None, TaskPriority::IoLow, async move {
            recycle_snapshot(receiver).await;
        });

        let root_dir = root_dir.as_ref();
        let mut replicas = HashMap::new();
        let mut num_snaps = 0;
        for (replica_id, replica_dir) in list_numeric_path(root_dir)? {
            for (index, snap_dir) in list_numeric_path(&replica_dir)? {
                let meta_name = snap_dir.join(SNAP_META);
                if !std::fs::try_exists(&meta_name)? {
                    warn!("replica {replica_id} recycles snap {index} since {SNAP_META} is not exists, dir {}",
                        snap_dir.display());
                    sender
                        .start_send((replica_id, snap_dir))
                        .unwrap_or_default();
                    continue;
                }
                let bytes = std::fs::read(&meta_name)?;
                let snapshot_meta = match SnapshotMeta::decode(&*bytes) {
                    Ok(meta) => meta,
                    Err(e) => {
                        warn!("replica {replica_id} recycles snap {index} since decode {SNAP_META}: {e}");
                        sender
                            .start_send((replica_id, snap_dir))
                            .unwrap_or_default();
                        continue;
                    }
                };

                info!(
                    "replica {replica_id} recovers snap {index}, dir {}",
                    snap_dir.display()
                );

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
                replica_mgr.push(info);
                num_snaps += 1;
            }
        }

        info!(
            "snap manager recovers {} replicas {num_snaps} snaps",
            replicas.len()
        );

        Ok(SnapManager {
            shared: Arc::new(SnapManagerShared {
                root_dir: root_dir.to_owned(),
                min_keep_intervals: Duration::from_secs(180),
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
                let name = dir_name.file_name().unwrap().to_string_lossy().into_owned();
                let snapshot_index = name.parse::<usize>().expect("install invalid snapshot dir");
                let snapshot_id = format!("{}", snapshot_index).as_bytes().to_owned();
                debug_assert!(snapshot_index < replica.next_snapshot_index);

                info!(
                    "replica {replica_id} install snap {snapshot_index}, dir {}",
                    dir_name.display()
                );

                replica.push(SnapshotInfo {
                    snapshot_id: snapshot_id.clone(),
                    base_dir: replica.base_dir.join(name),
                    meta: meta.clone(),
                    ref_count: 0,
                    created_at: Instant::now(),
                });
                snapshot_id
            }
            _ => panic!("install invalid snapshot dir: {}", dir_name.display()),
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

    pub fn recycle_snapshots(&self, replica_id: u64, mode: RecycleSnapMode) {
        let now = Instant::now();
        let (mut sender, snapshots) = {
            let mut inner = self.shared.inner.lock().unwrap();
            let replica = inner.replicas.get_mut(&replica_id);
            if replica.is_none() {
                return;
            }

            let replica = replica.unwrap();
            let snapshots = match mode {
                RecycleSnapMode::RequiredIndex(required_index) => replica
                    .snapshots
                    .drain_filter(|info| {
                        info.meta.apply_state.as_ref().unwrap().index < required_index
                            && info.created_at + self.shared.min_keep_intervals < now
                            && info.ref_count == 0
                    })
                    .map(|info| info.base_dir)
                    .collect::<Vec<_>>(),
                RecycleSnapMode::All => {
                    let snapshots = replica
                        .snapshots
                        .iter()
                        .map(|info| info.base_dir.clone())
                        .collect::<Vec<_>>();
                    inner.replicas.remove(&replica_id);
                    snapshots
                }
            };
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

    fn push(&mut self, info: SnapshotInfo) {
        let index = self
            .snapshots
            .partition_point(|i| i.snapshot_id < info.snapshot_id);
        self.snapshots.insert(index, info);
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

    // The order in which filenames are read by successive calls to `readdir()` depends on the
    // filesystem implementation.
    values.sort_unstable();

    Ok(values)
}

async fn recycle_snapshot(mut receiver: mpsc::UnboundedReceiver<(u64, PathBuf)>) {
    while let Some((replica_id, snapshot_dir)) = receiver.next().await {
        if let Err(err) = std::fs::remove_dir_all(&snapshot_dir) {
            error!(
                "replica {replica_id} recycle snapshot {}: {err}",
                snapshot_dir.display(),
            );
            continue;
        }

        info!(
            "replica {replica_id} recycle snapshot {}",
            snapshot_dir.display()
        );
        // Remove parent directory if it is empty.
        if let Some(parent) = snapshot_dir.parent() {
            std::fs::remove_dir(parent).unwrap_or_default();
        }
    }
}

#[cfg(test)]
mod tests {
    use engula_api::server::v1::GroupDesc;
    use tempdir::TempDir;

    use super::*;
    use crate::{
        raftgroup::SnapshotBuilder,
        runtime::{time::sleep, ExecutorOwner},
        serverpb::v1::ApplyState,
    };

    struct SimpleSnapshotBuilder {
        index: u64,
        content: Vec<u8>,
    }

    #[crate::async_trait]
    impl SnapshotBuilder for SimpleSnapshotBuilder {
        async fn checkpoint(&self, base_dir: &Path) -> Result<(ApplyState, GroupDesc)> {
            info!("create snapshot at: {}", base_dir.display());
            if let Some(parent) = base_dir.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(base_dir, &self.content)?;
            info!("write snapshot content");
            let state = ApplyState {
                index: self.index,
                term: 0,
            };
            Ok((state, GroupDesc::default()))
        }
    }

    struct MultiFilesSnapshotBuilder {
        index: u64,
        content_1: Vec<u8>,
        content_2: Vec<u8>,
    }

    #[crate::async_trait]
    impl SnapshotBuilder for MultiFilesSnapshotBuilder {
        async fn checkpoint(&self, base_dir: &Path) -> Result<(ApplyState, GroupDesc)> {
            info!("create snapshot at: {}", base_dir.display());
            std::fs::create_dir_all(base_dir)?;
            let file_1 = base_dir.join("1");
            let file_2 = base_dir.join("2");
            std::fs::write(file_1, &self.content_1)?;
            std::fs::write(file_2, &self.content_2)?;
            info!("write snapshot content");
            let state = ApplyState {
                index: self.index,
                term: 0,
            };
            Ok((state, GroupDesc::default()))
        }
    }

    async fn build_snapshot(
        manager: &SnapManager,
        replica_id: u64,
        index: u64,
        content: Vec<u8>,
    ) -> Vec<u8> {
        let builder: Box<dyn SnapshotBuilder> = Box::new(SimpleSnapshotBuilder { index, content });
        create::create_snapshot(replica_id, manager, builder)
            .await
            .unwrap()
    }

    #[test]
    fn recovery() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            let root_dir = TempDir::new("snap-recovery").unwrap();
            std::fs::create_dir_all(&root_dir).unwrap();

            let replica_id_1: u64 = 1;
            let replica_id_2: u64 = 2;
            let snap_manager = SnapManager::recovery(&root_dir).await.unwrap();

            let snap_id_1 = build_snapshot(&snap_manager, replica_id_1, 1, vec![1]).await;
            let snap_id_2 = build_snapshot(&snap_manager, replica_id_1, 2, vec![2]).await;
            let snap_id_3 = build_snapshot(&snap_manager, replica_id_1, 3, vec![3]).await;
            let replica_snaps_1 = vec![snap_id_1, snap_id_2, snap_id_3.clone()];

            drop(snap_manager);

            let snap_manager = SnapManager::recovery(&root_dir).await.unwrap();
            for snap_id in &replica_snaps_1 {
                assert!(
                    snap_manager
                        .lock_snap(replica_id_1, snap_id.as_slice())
                        .is_some(),
                    "snap id is {snap_id:?}"
                );
            }
            let snap = snap_manager.latest_snap(replica_id_1);
            assert!(snap.is_some());
            let snap = snap.unwrap();
            info!("the latest snapshot id is {:?}", snap.snapshot_id);
            assert_eq!(snap.snapshot_id, snap_id_3);

            assert!(snap_manager.latest_snap(replica_id_2).is_none());
        });
    }

    #[test]
    fn send_and_save_snapshot() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            let root_dir = TempDir::new("download-snapshot").unwrap();
            std::fs::create_dir_all(&root_dir).unwrap();

            let replica_id: u64 = 1;
            let snap_manager = SnapManager::recovery(&root_dir).await.unwrap();

            // Prepare snapshot
            let content = vec![1, 2, 3, 4, 5, 6, 7];
            let snap_id = build_snapshot(&snap_manager, replica_id, 0, content.clone()).await;

            // Send snapshot on leader side.
            let snapshot_chunk_stream = send::send_snapshot(&snap_manager, replica_id, snap_id)
                .await
                .unwrap();

            // Save snapshot on follower side.
            let new_snap_id =
                download::save_snapshot(&snap_manager, replica_id + 1, snapshot_chunk_stream)
                    .await
                    .unwrap();

            info!("new snap id is {new_snap_id:?}");

            // Validate snapshot content.
            let snap = snap_manager.lock_snap(replica_id + 1, &new_snap_id);
            assert!(snap.is_some());
            let snap = snap.unwrap();
            let data = snap.base_dir.join(SNAP_DATA);
            let received_content = std::fs::read_to_string(&data).unwrap();
            assert_eq!(received_content.as_bytes(), content.as_slice());
        });
    }

    #[test]
    fn send_and_save_snapshot_with_multi_files() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            let root_dir = TempDir::new("download-snapshot-multi-files").unwrap();
            std::fs::create_dir_all(&root_dir).unwrap();

            let replica_id: u64 = 1;
            let snap_manager = SnapManager::recovery(&root_dir).await.unwrap();

            // Prepare snapshot
            let content_1 = vec![1, 2, 3, 4, 5, 6, 7, 1];
            let content_2 = vec![1, 2, 3, 4, 5, 6, 7, 2];

            let builder: Box<dyn SnapshotBuilder> = Box::new(MultiFilesSnapshotBuilder {
                index: 1,
                content_1: content_1.clone(),
                content_2: content_2.clone(),
            });
            let snap_id = create::create_snapshot(replica_id, &snap_manager, builder)
                .await
                .unwrap();

            // Send snapshot on leader side.
            let snapshot_chunk_stream = send::send_snapshot(&snap_manager, replica_id, snap_id)
                .await
                .unwrap();

            // Save snapshot on follower side.
            let new_snap_id =
                download::save_snapshot(&snap_manager, replica_id + 1, snapshot_chunk_stream)
                    .await
                    .unwrap();

            info!("new snap id is {new_snap_id:?}");

            // Validate snapshot content.
            let snap = snap_manager.lock_snap(replica_id + 1, &new_snap_id);
            assert!(snap.is_some());
            let snap = snap.unwrap();
            let data = snap.base_dir.join(SNAP_DATA);
            let file_1 = data.join("1");
            let file_2 = data.join("2");
            let received_content = std::fs::read_to_string(&file_1).unwrap();
            assert_eq!(received_content.as_bytes(), content_1.as_slice());
            let received_content = std::fs::read_to_string(&file_2).unwrap();
            assert_eq!(received_content.as_bytes(), content_2.as_slice());
        });
    }

    #[test]
    fn recycle() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            let root_dir = TempDir::new("snap-recycle").unwrap();
            std::fs::create_dir_all(&root_dir).unwrap();

            let replica_id: u64 = 1;
            let snap_manager = SnapManager::new(root_dir.path().to_owned());

            let snap_id_1 = build_snapshot(&snap_manager, replica_id, 1, vec![1]).await;
            let snap_id_2 = build_snapshot(&snap_manager, replica_id, 2, vec![2]).await;
            let snap_id_3 = build_snapshot(&snap_manager, replica_id, 3, vec![3]).await;

            sleep(Duration::from_millis(1)).await;

            let snap = snap_manager.lock_snap(replica_id, &snap_id_2);
            assert!(snap.is_some());

            snap_manager.recycle_snapshots(replica_id, RecycleSnapMode::RequiredIndex(3));
            assert!(snap_manager.lock_snap(replica_id, &snap_id_1).is_none());
            assert!(snap_manager.lock_snap(replica_id, &snap_id_2).is_some());
            assert!(snap_manager.lock_snap(replica_id, &snap_id_3).is_some());

            drop(snap);

            snap_manager.recycle_snapshots(replica_id, RecycleSnapMode::RequiredIndex(3));
            assert!(snap_manager.lock_snap(replica_id, &snap_id_1).is_none());
            assert!(snap_manager.lock_snap(replica_id, &snap_id_2).is_none());
            assert!(snap_manager.lock_snap(replica_id, &snap_id_3).is_some());
        });
    }

    #[test]
    fn ordered_install() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            let root_dir = TempDir::new("snap-install-order").unwrap();
            std::fs::create_dir_all(&root_dir).unwrap();

            let replica_id: u64 = 1;
            let snap_mgr = SnapManager::new(root_dir.path().to_owned());

            let snap_dir_1 = snap_mgr.create(replica_id);
            let snap_dir_2 = snap_mgr.create(replica_id);
            let snap_meta = SnapshotMeta {
                apply_state: Some(ApplyState::default()),
                group_desc: Some(GroupDesc::default()),
                files: vec![],
            };

            // Install snap in reversed orders.
            let expected_id = snap_mgr.install(replica_id, &snap_dir_2, &snap_meta);
            snap_mgr.install(replica_id, &snap_dir_1, &snap_meta);

            assert!(matches!(snap_mgr.latest_snap(replica_id),
                Some(info) if info.snapshot_id == expected_id));
        });
    }

    #[test]
    fn recycle_during_creating() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            let root_dir = TempDir::new("snap-recycle-during-creating").unwrap();
            std::fs::create_dir_all(&root_dir).unwrap();

            let replica_id: u64 = 1;
            let snap_mgr = SnapManager::new(root_dir.path().to_owned());

            let snap_dir_1 = snap_mgr.create(replica_id);
            let snap_meta = SnapshotMeta {
                apply_state: Some(ApplyState::default()),
                group_desc: Some(GroupDesc::default()),
                files: vec![],
            };
            snap_mgr.recycle_snapshots(replica_id, RecycleSnapMode::RequiredIndex(123123));
            snap_mgr.install(replica_id, &snap_dir_1, &snap_meta);
        });
    }
}
