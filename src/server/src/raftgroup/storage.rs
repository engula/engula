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
    cell::{Cell, RefCell},
    collections::VecDeque,
    sync::Arc,
};

use engula_api::server::v1::*;
use prost::Message;
use raft::{prelude::*, GetEntriesContext, RaftState};
use raft_engine::{Command, Engine, LogBatch, MessageExt};
use tracing::{debug, error};

use super::{node::WriteTask, snap::SnapManager, RaftConfig};
use crate::{
    serverpb::v1::{EntryId, EvalResult, RaftLocalState},
    Result,
};

#[derive(Clone)]
pub struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

/// `EntryCache` is used to make up for the design of raft-rs. All logs that have not been applied
/// are stored in `EntryCache`. Therefore, it can be guaranteed that in addition to replicate
/// entries, other operations in raft-rs that need to read entries and their attributes directly
/// should hit the EntryCache directly, and do not need to be read from disk.
struct EntryCache {
    entries: VecDeque<Entry>,
}

/// The implementation of [`raft::Storage`].
pub struct Storage {
    replica_id: u64,

    first_index: u64,
    last_index: u64,
    cache: EntryCache,
    local_state: RaftLocalState,
    hard_state: HardState,
    initial_conf_state: RefCell<Option<ConfState>>,

    pub create_snapshot: Cell<bool>,
    pub is_creating_snapshot: Cell<bool>,
    snap_mgr: SnapManager,
    engine: Arc<Engine>,
}

impl Storage {
    pub async fn open(
        cfg: &RaftConfig,
        replica_id: u64,
        applied_index: u64,
        conf_state: ConfState,
        engine: Arc<Engine>,
        snap_mgr: SnapManager,
    ) -> Result<Self> {
        let hard_state = engine
            .get_message::<HardState>(replica_id, keys::HARD_STATE_KEY)?
            .expect("hard state must be initialized");
        let local_state = engine
            .get_message::<RaftLocalState>(replica_id, keys::LOCAL_STATE_KEY)?
            .expect("raft local state must be initialized");

        let mut first_index = engine.first_index(replica_id).unwrap_or(1);
        let mut last_index = engine.last_index(replica_id).unwrap_or(0);
        if let Some(truncated) = local_state.last_truncated.clone() {
            if first_index <= last_index {
                if truncated.index + 1 != first_index {
                    panic!("some log entries are missing, truncated state {truncated:?}, engine range [{}, {})",
                        first_index, last_index + 1);
                }
            } else {
                // update empty range.
                first_index = truncated.index + 1;
                last_index = truncated.index;
            }
        }

        let cache = if applied_index < last_index {
            // There exists some entries haven't been applied.
            let mut applied_index = applied_index;
            if cfg.testing_knobs.force_new_peer_receiving_snapshot && applied_index == 0 {
                applied_index = 5;
            }

            assert!(first_index <= applied_index + 1,
                "there are some missing entries, applied index {applied_index}, entries [{first_index}, {})", last_index + 1);

            debug!(
                "replica {replica_id} fetch uncommitted entries in range [{}, {})",
                applied_index + 1,
                last_index + 1
            );
            let mut entries = vec![];
            engine.fetch_entries_to::<MessageExtTyped>(
                replica_id,
                applied_index + 1,
                last_index + 1,
                None,
                &mut entries,
            )?;
            EntryCache::with_entries(entries)
        } else {
            EntryCache::new()
        };

        debug!(
            "replica {replica_id} open storage with applied index {applied_index}, log range [{}, {}), local state {local_state:?}",
            first_index,
            last_index + 1,
        );

        Ok(Storage {
            replica_id,

            first_index,
            last_index,
            cache,
            hard_state,
            initial_conf_state: RefCell::new(Some(conf_state)),
            local_state,
            snap_mgr,
            create_snapshot: Cell::new(false),
            is_creating_snapshot: Cell::new(false),
            engine,
        })
    }

    /// Apply [`WriteTask`] to [`LogBatch`], and save some states to storage.
    pub fn write(&mut self, batch: &mut LogBatch, write_task: &WriteTask) -> Result<()> {
        if let Some(hs) = &write_task.hard_state {
            batch
                .put_message(self.replica_id, keys::HARD_STATE_KEY.to_owned(), hs)
                .unwrap();
            self.hard_state = hs.clone();
        }

        if let Some(snapshot) = &write_task.snapshot {
            assert!(
                write_task.entries.is_empty(),
                "Does not stable entries during appling snapshot"
            );
            batch.add_command(self.replica_id, Command::Clean);
            let metadata = snapshot.get_metadata();
            let raft_local_state = RaftLocalState {
                replica_id: self.replica_id,
                last_truncated: Some(EntryId {
                    index: metadata.index,
                    term: metadata.term,
                }),
            };
            batch
                .put_message(
                    self.replica_id,
                    keys::LOCAL_STATE_KEY.to_owned(),
                    &raft_local_state,
                )
                .unwrap();
            self.hard_state.set_commit(metadata.index);
            batch
                .put_message(
                    self.replica_id,
                    keys::HARD_STATE_KEY.to_owned(),
                    &self.hard_state,
                )
                .unwrap();
            self.cache = EntryCache::new();
            self.first_index = metadata.index + 1;
            self.last_index = metadata.index;
            self.local_state = raft_local_state;
        } else if !write_task.entries.is_empty() {
            batch
                .add_entries::<MessageExtTyped>(self.replica_id, &write_task.entries)
                .unwrap();
            self.cache.append(&write_task.entries);
            self.last_index = write_task.entries.last().unwrap().index;
        }

        Ok(())
    }

    #[inline]
    pub fn post_apply(&mut self, applied_index: u64) {
        self.cache.drains_to(applied_index);
    }

    #[inline]
    pub fn compact_to(&mut self, to: u64) -> LogBatch {
        use raft::Storage;

        debug_assert!(to > self.first_index);
        self.first_index = to;

        // `term()` allowed range is `[first_index-1, last_index]`.
        let truncated_index = to.checked_sub(1).unwrap();
        let term = self.term(truncated_index).unwrap();
        let local_state = RaftLocalState {
            replica_id: self.replica_id,
            last_truncated: Some(EntryId {
                index: truncated_index,
                term,
            }),
        };

        let mut lb = LogBatch::default();
        // The semantics of `Command::Compact` is removing all entries with index smaller than
        // `index`.
        lb.add_command(self.replica_id, Command::Compact { index: to });
        lb.put_message(
            self.replica_id,
            keys::LOCAL_STATE_KEY.to_owned(),
            &local_state,
        )
        .unwrap();

        // Since the commit field of HardState is lazily updated, it must also be consistent with
        // the log range.
        if self.first_index > self.hard_state.commit {
            self.hard_state.commit = self.first_index;
            lb.put_message(
                self.replica_id,
                keys::LOCAL_STATE_KEY.to_owned(),
                &self.hard_state,
            )
            .unwrap();
        }

        self.local_state = local_state;
        lb
    }

    #[inline]
    fn last_truncated_entry_id(&self) -> EntryId {
        self.local_state.last_truncated.clone().unwrap_or_default()
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.last_truncated_entry_id().index
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.last_truncated_entry_id().term
    }

    #[inline]
    pub fn range(&self) -> std::ops::Range<u64> {
        self.first_index..(self.last_index + 1)
    }

    fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            panic!("low {} is greater than high {}", low, high);
        } else if high > self.last_index + 1 {
            panic!(
                "entries high {} is out of bound, last index {}",
                high, self.last_index
            );
        } else if low <= self.truncated_index() {
            Err(raft::Error::Store(raft::StorageError::Compacted))
        } else {
            Ok(())
        }
    }
}

impl raft::Storage for Storage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let conf_state = match self.initial_conf_state.borrow_mut().take() {
            Some(cs) => cs,
            None => panic!("invoke initial state multiple times"),
        };
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.check_range(low, high)?;
        let mut entries = Vec::with_capacity((high - low) as usize);
        if low == high {
            return Ok(entries);
        }

        let max_size = max_size.into().unwrap_or(u64::MAX) as usize;
        let cache_low = self.cache.first_index().unwrap_or(u64::MAX);
        if cache_low > low {
            // TODO(walter) support load entries asynchronously.
            if let Err(e) = self.engine.fetch_entries_to::<MessageExtTyped>(
                self.replica_id,
                low,
                std::cmp::min(cache_low, high),
                Some(max_size),
                &mut entries,
            ) {
                error!(
                    replica = self.replica_id,
                    "fetch entries from engine: {}", e
                );
                return Err(other_store_error(e));
            }
        }

        if cache_low < high {
            self.cache.fetch_entries_to(
                std::cmp::max(low, cache_low),
                high,
                max_size,
                &mut entries,
            );
        }

        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }

        self.check_range(idx, idx + 1)?;
        if !self
            .cache
            .first_index()
            .map(|fi| fi <= idx)
            .unwrap_or_default()
        {
            // TODO(walter) support fetch in asynchronously.
            match self
                .engine
                .get_entry::<MessageExtTyped>(self.replica_id, idx)
            {
                Err(e) => Err(other_store_error(e)),
                Ok(Some(entry)) => Ok(entry.get_term()),
                Ok(None) => Err(raft::Error::Store(raft::StorageError::Compacted)),
            }
        } else {
            Ok(self
                .cache
                .entry(idx)
                .map(|e| e.term)
                .expect("acquire term out of the range of cached entries"))
        }
    }

    #[inline]
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index)
    }

    #[inline]
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        if !self.is_creating_snapshot.get() {
            if let Some(snap_info) = self.snap_mgr.latest_snap(self.replica_id) {
                let snap_meta = &snap_info.meta;
                let apply_state = snap_meta.apply_state.clone().unwrap();
                if apply_state.index >= request_index {
                    return Ok(snap_info.to_raft_snapshot());
                }
            }

            assert!(!self.create_snapshot.get());
            self.create_snapshot.set(true);
            self.is_creating_snapshot.set(true);
        }

        Err(raft::Error::Store(
            raft::StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}

impl EntryCache {
    fn new() -> Self {
        EntryCache {
            entries: VecDeque::default(),
        }
    }

    fn with_entries(entries: Vec<Entry>) -> Self {
        EntryCache {
            entries: entries.into(),
        }
    }

    fn first_index(&self) -> Option<u64> {
        self.entries.front().map(|e| e.index)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        let first_index = self.entries.front()?.index;
        if index >= first_index {
            Some(&self.entries[(index - first_index) as usize])
        } else {
            None
        }
    }

    fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        max_size: usize,
        entries: &mut Vec<Entry>,
    ) -> usize {
        if begin >= end {
            return 0;
        }
        let limit = end.saturating_sub(begin) as usize;

        assert!(!self.entries.is_empty());
        let cache_low = self.entries.front().unwrap().get_index();
        let start_index = begin.checked_sub(cache_low).unwrap() as usize;

        let mut fetched_size: usize = 0;
        let end_index = start_index
            + self
                .entries
                .iter()
                .skip(start_index)
                .take(limit)
                .take_while(|e| {
                    fetched_size += e.encoded_len();
                    fetched_size <= max_size
                })
                .count();
        entries.extend(self.entries.range(start_index..end_index).cloned());
        fetched_size
    }

    pub fn append(&mut self, entries: &[Entry]) {
        if entries.is_empty() {
            return;
        }

        if let Some(cache_last_index) = self.entries.back().map(|e| e.get_index()) {
            let first_index = entries[0].get_index();
            if cache_last_index >= first_index {
                let cache_len = self.entries.len();
                let truncate_to = cache_len
                    .checked_sub((cache_last_index - first_index + 1) as usize)
                    .unwrap_or_default();
                self.entries.drain(truncate_to..);
            } else if cache_last_index + 1 < first_index {
                panic!(
                    "EntryCache::append unexpected hole: {} < {}",
                    cache_last_index, first_index
                );
            }
        }

        self.entries.extend(entries.iter().cloned());
    }

    pub fn drains_to(&mut self, applied_index: u64) {
        if let Some(cache_low) = self.entries.front().map(Entry::get_index) {
            let len = applied_index.checked_sub(cache_low).unwrap() as usize;
            self.entries.drain(0..(len / 2));
        }
    }
}

/// Write raft initial states into log engine.  All previous data of this raft will be clean first.
///
/// `voters` and `initial_eval_results` will be committed to raft as committed data. These logs are
/// applied when raft is restarted.
#[allow(clippy::field_reassign_with_default)]
pub async fn write_initial_state(
    cfg: &RaftConfig,
    engine: &Engine,
    replica_id: u64,
    mut replicas: Vec<ReplicaDesc>,
    initial_eval_results: Vec<EvalResult>,
) -> Result<()> {
    let mut initial_entries = vec![];
    let mut initial_index = 0;
    if cfg.testing_knobs.force_new_peer_receiving_snapshot
        && !(replicas.is_empty() && initial_eval_results.is_empty())
    {
        initial_index = 5;
    }

    let mut last_index: u64 = initial_index;
    if !replicas.is_empty() {
        replicas.sort_unstable_by_key(|r| r.id);

        // The underlying `raft-rs` crate isn't allow empty configs enter joint state,
        // so have to add replicas one by one.
        for replica in replicas {
            let replica_id = replica.id;
            let node_id = replica.node_id;
            last_index += 1;
            let change_replicas = ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: if replica.role == ReplicaRole::Learner as i32 {
                        ChangeReplicaType::AddLearner.into()
                    } else {
                        ChangeReplicaType::Add.into()
                    },
                    replica_id,
                    node_id,
                }],
            };
            let conf_change = super::encode_to_conf_change(change_replicas);
            let mut e = Entry::default();
            e.index = last_index;
            e.term = 1;
            e.set_entry_type(EntryType::EntryConfChangeV2);
            e.data = conf_change.encode_to_vec();
            e.context = vec![];
            initial_entries.push(e);
        }
    }

    if !initial_eval_results.is_empty() {
        for eval_result in initial_eval_results {
            last_index += 1;
            let mut ent = Entry::default();
            ent.index = last_index;
            ent.term = 1;
            ent.data = eval_result.encode_to_vec();
            ent.context = vec![];
            initial_entries.push(ent);
        }
    }

    let mut hard_state = HardState::default();
    let local_state = RaftLocalState {
        replica_id,
        last_truncated: Some(EntryId {
            index: initial_index,
            term: 0,
        }),
    };
    if !initial_entries.is_empty() {
        // Due to the limitations of the raft-rs implementation, 0 cannot be used here as the term
        // of initial entries. Therefore, it is necessary to set the `vote` and `term` in the
        // HardState to a value that ensures that the current term will not elect a new leader.
        hard_state.commit = last_index;
        hard_state.vote = replica_id;
        hard_state.term = 1;
    }

    let mut batch = LogBatch::default();
    batch.add_command(replica_id, Command::Clean);
    batch
        .add_entries::<MessageExtTyped>(replica_id, &initial_entries)
        .unwrap();
    batch
        .put_message(replica_id, keys::HARD_STATE_KEY.to_owned(), &hard_state)
        .unwrap();
    batch
        .put_message(replica_id, keys::LOCAL_STATE_KEY.to_owned(), &local_state)
        .unwrap();

    debug!(
        "write initial state of {}, total {} initial entries",
        replica_id,
        initial_entries.len()
    );

    engine.write(&mut batch, true)?;
    Ok(())
}

pub mod keys {
    pub const HARD_STATE_KEY: &[u8] = b"hard_state";
    pub const LOCAL_STATE_KEY: &[u8] = b"local_state";
}

fn other_store_error(e: raft_engine::Error) -> raft::Error {
    raft::Error::Store(raft::StorageError::Other(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use raft_engine::{Config, Engine};
    use tempdir::TempDir;
    use tracing::info;

    use super::*;
    use crate::runtime::*;

    fn mocked_entries(select_term: Option<u64>) -> Vec<(u64, u64)> {
        let entries = vec![
            // term 1
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            // term 2
            (5, 2),
            (6, 2),
            (7, 2),
            (8, 2),
            // term 3
            (9, 3),
            (10, 3),
            (11, 3),
            (12, 3),
        ];
        if let Some(expect) = select_term {
            entries
                .into_iter()
                .filter(|(_, term)| *term == expect)
                .collect()
        } else {
            entries
        }
    }

    async fn insert_entries(engine: Arc<Engine>, storage: &mut Storage, entries: Vec<(u64, u64)>) {
        let entries: Vec<Entry> = entries
            .into_iter()
            .map(|(idx, term)| {
                let mut e = Entry::default();
                e.set_index(idx);
                e.set_term(term);
                e
            })
            .collect();

        let mut lb = LogBatch::default();
        let wt = WriteTask::with_entries(entries);
        storage.write(&mut lb, &wt).unwrap();
        engine.write(&mut lb, false).unwrap();
    }

    fn validate_term(storage: &impl raft::Storage, entries: Vec<(u64, u64)>) {
        for (idx, term) in entries {
            assert_eq!(storage.term(idx).unwrap(), term);
        }
    }

    fn validate_entries(storage: &impl raft::Storage, entries: Vec<(u64, u64)>) {
        for (idx, term) in entries {
            println!("index is {} term {}", idx, term);
            let read_entries = storage
                .entries(idx, idx + 1, None, GetEntriesContext::empty(false))
                .unwrap();
            assert!(!read_entries.is_empty());
            assert_eq!(read_entries.len(), 1);
            assert_eq!(read_entries[0].get_term(), term);
        }
    }

    fn validate_range(storage: &impl raft::Storage, first_index: u64, last_index: u64) {
        assert_eq!(storage.first_index(), Ok(first_index));
        assert_eq!(storage.last_index(), Ok(last_index));
    }

    fn validate_compacted(
        storage: &impl raft::Storage,
        index: u64,
        truncated_index: u64,
        truncated_term: u64,
    ) {
        if index == truncated_index {
            assert!(matches!(storage.term(index), Ok(v) if v == truncated_term));
        } else {
            assert!(matches!(
                storage.term(index),
                Err(raft::Error::Store(raft::StorageError::Compacted))
            ));
        }
        assert!(matches!(
            storage.entries(index, index + 1, None, GetEntriesContext::empty(false)),
            Err(raft::Error::Store(raft::StorageError::Compacted))
        ));
    }

    async fn raft_storage_apply_snapshot() {
        let dir = TempDir::new("raft-storage-apply-snapshot").unwrap();

        let cfg = Config {
            dir: dir.path().join("db").to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(cfg).unwrap());

        write_initial_state(&RaftConfig::default(), engine.as_ref(), 1, vec![], vec![])
            .await
            .unwrap();

        let snap_mgr = SnapManager::new(dir.path().join("snap"));
        let mut storage = Storage::open(
            &RaftConfig::default(),
            1,
            0,
            ConfState::default(),
            engine.clone(),
            snap_mgr,
        )
        .await
        .unwrap();
        insert_entries(engine.clone(), &mut storage, mocked_entries(None)).await;
        validate_term(&storage, mocked_entries(None));
        validate_entries(&storage, mocked_entries(None));
        let first_index = mocked_entries(None).first().unwrap().0;
        let last_index = mocked_entries(None).last().unwrap().0;
        validate_range(&storage, first_index, last_index);

        let snap_index = 123;
        let snap_term = 123;
        let mut task = WriteTask::with_entries(vec![]);
        task.snapshot = Some(Snapshot {
            data: vec![],
            metadata: Some(SnapshotMetadata {
                conf_state: None,
                index: snap_index,
                term: snap_term,
            }),
        });

        let mut lb = LogBatch::default();
        storage.write(&mut lb, &task).unwrap();
        assert_eq!(storage.hard_state.commit, snap_index);
        assert_eq!(storage.truncated_index(), snap_index);
        assert_eq!(storage.truncated_term(), snap_term);
        assert_eq!(storage.first_index, snap_index + 1);
        assert_eq!(storage.last_index, snap_index);
    }

    async fn open_empty_raft_storage_after_applying_snapshot() {
        let dir = TempDir::new("open-empty-raft-storage-after-applying-snapshot").unwrap();

        let cfg = Config {
            dir: dir.path().join("db").to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(cfg).unwrap());

        write_initial_state(&RaftConfig::default(), engine.as_ref(), 1, vec![], vec![])
            .await
            .unwrap();

        let snap_mgr = SnapManager::new(dir.path().join("snap"));
        let mut storage = Storage::open(
            &RaftConfig::default(),
            1,
            0,
            ConfState::default(),
            engine.clone(),
            snap_mgr.clone(),
        )
        .await
        .unwrap();

        let snap_index = 123;
        let snap_term = 123;
        let mut task = WriteTask::with_entries(vec![]);
        task.snapshot = Some(Snapshot {
            data: vec![],
            metadata: Some(SnapshotMetadata {
                conf_state: None,
                index: snap_index,
                term: snap_term,
            }),
        });

        let mut lb = LogBatch::default();
        storage.write(&mut lb, &task).unwrap();
        assert_eq!(storage.hard_state.commit, snap_index);
        assert_eq!(storage.truncated_index(), snap_index);
        assert_eq!(storage.truncated_term(), snap_term);
        assert_eq!(storage.first_index, snap_index + 1);
        assert_eq!(storage.last_index, snap_index);
        engine.write(&mut lb, false).unwrap();

        info!("open storage again after applying snapshot");

        // Open storage again.
        drop(storage);
        let storage = Storage::open(
            &RaftConfig::default(),
            1,
            snap_index,
            ConfState::default(),
            engine.clone(),
            snap_mgr,
        )
        .await
        .unwrap();
        assert_eq!(storage.hard_state.commit, snap_index);
        assert_eq!(storage.truncated_index(), snap_index);
        assert_eq!(storage.truncated_term(), snap_term);
        assert_eq!(storage.first_index, snap_index + 1);
        assert_eq!(storage.last_index, snap_index);
    }

    async fn raft_storage_inner() {
        let dir = TempDir::new("raft-storage").unwrap();

        let cfg = Config {
            dir: dir.path().join("db").to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(cfg).unwrap());

        write_initial_state(&RaftConfig::default(), engine.as_ref(), 1, vec![], vec![])
            .await
            .unwrap();

        let snap_mgr = SnapManager::new(dir.path().join("snap"));
        let mut storage = Storage::open(
            &RaftConfig::default(),
            1,
            0,
            ConfState::default(),
            engine.clone(),
            snap_mgr,
        )
        .await
        .unwrap();
        insert_entries(engine.clone(), &mut storage, mocked_entries(None)).await;
        validate_term(&storage, mocked_entries(None));
        validate_entries(&storage, mocked_entries(None));
        let first_index = mocked_entries(None).first().unwrap().0;
        let last_index = mocked_entries(None).last().unwrap().0;
        validate_range(&storage, first_index, last_index);

        // 1. apply all entries in term 1.
        storage.post_apply(mocked_entries(Some(1)).last().unwrap().0);
        validate_term(&storage, mocked_entries(None));
        validate_entries(&storage, mocked_entries(None));
        validate_range(&storage, first_index, last_index);

        // 2. apply all entries in term 2.
        storage.post_apply(mocked_entries(Some(2)).last().unwrap().0);
        validate_term(&storage, mocked_entries(None));
        validate_entries(&storage, mocked_entries(None));
        validate_range(&storage, first_index, last_index);

        // 3. compact entries to term 2.
        let compact_to = mocked_entries(Some(2)).first().unwrap().0;
        println!("compact to {}", compact_to);
        let mut lb = storage.compact_to(compact_to);
        engine.write(&mut lb, false).unwrap();
        validate_term(&storage, mocked_entries(Some(2)));
        validate_term(&storage, mocked_entries(Some(3)));
        validate_entries(&storage, mocked_entries(Some(2)));
        validate_entries(&storage, mocked_entries(Some(3)));
        let first_index = compact_to;
        validate_range(&storage, first_index, last_index);
        assert!(storage.hard_state.commit >= first_index);

        // Accesssing compacted entries should returns Error.
        let compacted_entry_index = mocked_entries(Some(1)).last().unwrap().0;
        assert_eq!(storage.truncated_index(), compacted_entry_index);
        validate_compacted(
            &storage,
            compacted_entry_index - 1,
            storage.truncated_index(),
            storage.truncated_term(),
        );
        validate_compacted(
            &storage,
            compacted_entry_index - 2,
            storage.truncated_index(),
            storage.truncated_term(),
        );
    }

    #[test]
    fn raft_storage_basic() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            raft_storage_inner().await;
        });
    }

    #[test]
    fn raft_storage_snapshot() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async move {
            raft_storage_apply_snapshot().await;
            open_empty_raft_storage_after_applying_snapshot().await;
        });
    }
}
