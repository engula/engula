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

use engula_api::server::v1::{ChangeReplica, ChangeReplicaType, ChangeReplicas};
use prost::Message;
use raft::{prelude::*, GetEntriesContext, RaftState};
use raft_engine::{Command, Engine, LogBatch, MessageExt};
use tracing::{debug, error};

use super::{node::WriteTask, snap::SnapManager};
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

// /// AsyncFetchTask contains the context for asynchronously fetching entries.
// pub struct AsyncFetchTask {
//     pub context: GetEntriesContext,
//     pub max_size: usize,
//     pub range: Range<u64>,
//     pub cached_entries: Vec<Entry>,
// }

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

        let first_index = engine.first_index(replica_id).unwrap_or(1);
        let last_index = engine.last_index(replica_id).unwrap_or(0);

        let cache = if applied_index < last_index {
            // There exists some entries haven't been applied.
            debug!(
                "replica {} fetch uncommitted entries in range [{}, {})",
                replica_id,
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
            "open storage of replica {} applied index {}, log range [{}, {})",
            replica_id,
            applied_index,
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
            self.cache = EntryCache::new();
            self.first_index = metadata.index;
            self.last_index = metadata.index;
        }

        if !write_task.entries.is_empty() {
            batch
                .add_entries::<MessageExtTyped>(self.replica_id, &write_task.entries)
                .unwrap();
            self.cache.append(&write_task.entries);
            self.last_index = write_task.entries.last().unwrap().index;
        }
        if let Some(hs) = &write_task.hard_state {
            batch
                .put_message(self.replica_id, keys::HARD_STATE_KEY.to_owned(), hs)
                .unwrap();
            self.hard_state = hs.clone();
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

        let term = self.term(to).unwrap();
        let local_state = RaftLocalState {
            replica_id: self.replica_id,
            last_truncated: Some(EntryId { index: to, term }),
        };
        let mut lb = LogBatch::default();
        lb.add_command(self.replica_id, Command::Compact { index: to });
        lb.put_message(
            self.replica_id,
            keys::LOCAL_STATE_KEY.to_owned(),
            &local_state,
        )
        .unwrap();
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
                cache_low,
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

        self.cache.fetch_entries_to(
            std::cmp::max(low, cache_low),
            std::cmp::max(high, cache_low),
            max_size,
            &mut entries,
        );

        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }

        self.check_range(idx, idx + 1)?;
        if self
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

            self.create_snapshot.set(true);
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

    #[allow(dead_code)]
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
    engine: &Engine,
    replica_id: u64,
    mut voters: Vec<(u64, u64)>,
    initial_eval_results: Vec<EvalResult>,
) -> Result<()> {
    let mut initial_entries = vec![];
    let mut last_index: u64 = 0;
    if !voters.is_empty() {
        voters.sort_unstable();

        // The underlying `raft-rs` crate isn't allow empty configs enter joint state,
        // so have to add replicas one by one.
        for (replica_id, node_id) in voters {
            last_index += 1;
            let change_replicas = ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::Add.into(),
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
        ..Default::default()
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
