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

use std::{cell::RefCell, collections::VecDeque, ops::Range, sync::Arc};

use prost::Message;
use raft::{prelude::*, GetEntriesContext, RaftState};
use raft_engine::{Engine, LogBatch, MessageExt};

use super::node::WriteTask;
use crate::{
    serverpb::v1::{EntryId, RaftLocalState},
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

/// AsyncFetchTask contains the context for asynchronously fetching entries.
pub struct AsyncFetchTask {
    pub context: GetEntriesContext,
    pub max_size: usize,
    pub range: Range<u64>,
    pub cached_entries: Vec<Entry>,
}

/// The implementation of [`raft::Storage`].
pub struct Storage {
    replica_id: u64,

    first_index: u64,
    last_index: u64,
    cache: EntryCache,
    local_state: RaftLocalState,
    hard_state: RefCell<HardState>,
    conf_state: RefCell<ConfState>,

    tasks: RefCell<Vec<AsyncFetchTask>>,
}

impl Storage {
    pub async fn open(replica_id: u64, applied_index: u64, engine: Arc<Engine>) -> Result<Self> {
        let hs = engine
            .get_message::<HardState>(replica_id, keys::HARD_STATE_KEY)?
            .expect("hard state must be initialized");
        let cs = engine
            .get_message::<ConfState>(replica_id, keys::CONF_STATE_KEY)?
            .expect("conf state must be initialized");
        let local_state = engine
            .get_message::<RaftLocalState>(replica_id, keys::LOCAL_STATE_KEY)?
            .expect("raft local state must be initialized");

        let first_index = engine.first_index(replica_id).unwrap_or(1);
        let last_index = engine.last_index(replica_id).unwrap_or(1);

        let cache = if applied_index < last_index {
            // There exists some entries haven't been applied.
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

        Ok(Storage {
            replica_id,

            first_index,
            last_index,
            cache,
            hard_state: RefCell::new(hs),
            conf_state: RefCell::new(cs),
            local_state,
            tasks: RefCell::new(vec![]),
        })
    }

    /// Apply [`WriteTask`] to [`LogBatch`], and save some states to storage.
    pub fn write(&mut self, batch: &mut LogBatch, write_task: &WriteTask) -> Result<()> {
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
            *self.hard_state.borrow_mut() = hs.clone();
        }
        Ok(())
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
        Ok(RaftState {
            hard_state: self.hard_state.borrow().clone(),
            conf_state: self.conf_state.borrow().clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        assert!(context.can_async(), "only support async");
        self.check_range(low, high)?;
        let mut entries = Vec::with_capacity((high - low) as usize);
        if low == high {
            return Ok(entries);
        }

        let max_size = max_size.into().unwrap_or(u64::MAX) as usize;
        let cache_low = self.cache.first_index().unwrap_or(u64::MAX);
        let fetched_size = self.cache.fetch_entries_to(
            std::cmp::max(low, cache_low),
            std::cmp::max(high, cache_low),
            max_size,
            &mut entries,
        );

        if cache_low <= low {
            Ok(entries)
        } else {
            // FIXME(walter) how to control the max size when loading from disk?
            let task = AsyncFetchTask {
                context,
                max_size: max_size.saturating_sub(fetched_size),
                range: low..std::cmp::min(high, cache_low),
                cached_entries: entries,
            };
            self.tasks.borrow_mut().push(task);
            Err(raft::Error::Store(
                raft::StorageError::LogTemporarilyUnavailable,
            ))
        }
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }

        self.check_range(idx, idx + 1)?;
        assert!(
            self.cache
                .first_index()
                .map(|fi| fi < idx)
                .unwrap_or_default(),
            "acquired term of index {} is out of the range of cached entries",
            idx
        );

        Ok(self
            .cache
            .entry(idx)
            .map(|e| e.term)
            .expect("acquire term out of the range of cached entries"))
    }

    #[inline]
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index)
    }

    #[inline]
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index)
    }

    #[allow(unused)]
    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        todo!()
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
        if index > first_index {
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
                    fetched_size >= max_size
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
}

/// Write raft initial states into log engine.  All previous data of this raft will be clean first.
pub async fn write_initial_state(engine: &Engine, replica_id: u64, voters: Vec<u64>) -> Result<()> {
    use raft_engine::Command;

    let hard_state = HardState::default();
    let conf_state = ConfState {
        voters,
        ..Default::default()
    };
    let local_state = RaftLocalState {
        replica_id,
        ..Default::default()
    };

    let mut batch = LogBatch::default();
    batch.add_command(replica_id, Command::Clean);
    batch
        .put_message(replica_id, keys::HARD_STATE_KEY.to_owned(), &hard_state)
        .unwrap();
    batch
        .put_message(replica_id, keys::CONF_STATE_KEY.to_owned(), &conf_state)
        .unwrap();
    batch
        .put_message(replica_id, keys::LOCAL_STATE_KEY.to_owned(), &local_state)
        .unwrap();

    engine.write(&mut batch, true)?;
    Ok(())
}

pub mod keys {
    pub const HARD_STATE_KEY: &[u8] = b"hard_state";
    pub const CONF_STATE_KEY: &[u8] = b"conf_state";
    pub const LOCAL_STATE_KEY: &[u8] = b"local_state";
}
