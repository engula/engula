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
    collections::{HashMap, HashSet},
    ops::Range,
};

use super::{message::*, MemStore, Progress};
use crate::{
    policy::{GroupReader, Policy as ReplicatePolicy},
    Entry, Sequence,
};

enum LearningState {
    None,
    Sealing {
        acked_indexes: Vec<u32>,
        /// Need send SEAL request to this target.
        pending: HashSet<String>,
    },
    Learning {
        actual_acked_index: u32,
        group_reader: GroupReader,
        /// Need send LEARN request to this target.
        pending: HashSet<String>,
    },
    Terminated,
}

/// A structure who responsible for sending received proposals to stores. There
/// will be one `Replicate` for each epoch, this `Replicate` is also responsible
/// for sealing segment and learning entries during recovery.
pub(super) struct Replicate {
    epoch_info: EpochInfo,
    policy: ReplicatePolicy,
    pub mem_store: MemStore,
    pub copy_set: HashMap<String, Progress>,

    sealed_set: HashSet<String>,
    learning_state: LearningState,
}

impl Replicate {
    pub fn new(
        epoch: u32,
        writer_epoch: u32,
        copy_set: Vec<String>,
        policy: ReplicatePolicy,
    ) -> Self {
        Replicate {
            epoch_info: EpochInfo {
                segment: epoch,
                writer: writer_epoch,
            },
            policy,
            mem_store: MemStore::new(epoch),
            copy_set: copy_set
                .into_iter()
                .map(|c| (c, Progress::new(epoch)))
                .collect(),
            sealed_set: HashSet::new(),
            learning_state: LearningState::None,
        }
    }

    pub fn recovery(
        epoch: u32,
        writer_epoch: u32,
        copy_set: Vec<String>,
        policy: ReplicatePolicy,
    ) -> Self {
        let pending = copy_set.iter().map(Clone::clone).collect();
        Replicate {
            learning_state: LearningState::Sealing {
                acked_indexes: Vec::default(),
                pending,
            },
            ..Replicate::new(epoch, writer_epoch, copy_set, policy)
        }
    }

    #[inline(always)]
    pub fn epoch(&self) -> u32 {
        self.epoch_info.segment
    }

    pub fn all_target_matched(&self) -> bool {
        let last_index = self.mem_store.next_index().saturating_sub(1);
        matches!(self.learning_state, LearningState::Terminated)
            && self.copy_set.iter().all(|(_, p)| p.is_matched(last_index))
    }

    fn replicate(
        &mut self,
        latest_tick: usize,
        acked_seq: Sequence,
        acked_index_advanced: bool,
        terminated: bool,
    ) -> Vec<Mutate> {
        let mut pending_writes = vec![];
        for (server_id, progress) in &mut self.copy_set {
            let next_index = self.mem_store.next_index();
            let (Range { start, mut end }, quota) = progress.next_chunk(next_index, latest_tick);
            let (acked_seq, entries, bytes) = match self.mem_store.range(start..end, quota) {
                Some((entries, bytes)) => {
                    // Do not forward acked sequence to unmatched index.
                    let matched_acked_seq =
                        Sequence::min(acked_seq, Sequence::new(self.epoch_info.segment, end - 1));
                    progress.replicate(end, 0);
                    (matched_acked_seq, entries, bytes)
                }
                // TODO(w41ter) support query indexes
                None if !terminated && acked_index_advanced => {
                    // All entries are replicated, might broadcast acked
                    // sequence.
                    (acked_seq, vec![], 0)
                }
                None => continue,
            };

            end = start + entries.len() as u32;
            let write = self.epoch_info.build_write(
                server_id.to_owned(),
                Write {
                    range: start..end,
                    bytes,
                    acked_seq,
                    entries,
                },
            );
            pending_writes.push(write);
        }
        pending_writes
    }

    pub fn broadcast(
        &mut self,
        latest_tick: usize,
        mut acked_seq: Sequence,
        acked_index_advanced: bool,
    ) -> (Vec<Mutate>, Vec<Learn>) {
        let mut pending_writes = vec![];
        let mut pending_learns = vec![];

        let mut terminated = false;
        let mut replicable = false;
        match &mut self.learning_state {
            LearningState::None | LearningState::Terminated => {
                terminated = matches!(self.learning_state, LearningState::Terminated);
                if terminated {
                    acked_seq = Sequence::new(self.epoch_info.writer, 0);
                }
                replicable = true;
            }
            LearningState::Sealing { pending, .. } => {
                for target in std::mem::take(pending) {
                    pending_writes.push(self.epoch_info.build_seal(target));
                }
            }
            LearningState::Learning {
                pending,
                actual_acked_index,
                group_reader,
            } => {
                // We would also replicate acked entries.
                replicable = true;
                let actual_acked_index = *actual_acked_index;
                for target in std::mem::take(pending) {
                    // Learn from the previous breakpoint. If no data has been read before, we need
                    // to start reading from acked_index, the reason is that we don't know whether a
                    // bridge entry has already been committed.
                    let start_index = group_reader.target_next_index(&target, actual_acked_index);
                    pending_learns.push(self.epoch_info.build_learn(target, start_index));
                }
            }
        }

        if replicable {
            let mut writes =
                self.replicate(latest_tick, acked_seq, acked_index_advanced, terminated);
            pending_writes.append(&mut writes);
        }

        (pending_writes, pending_learns)
    }

    /// Begin recovering from a normal replicate.
    pub fn become_recovery(&mut self, new_epoch: u32) {
        debug_assert!(matches!(self.learning_state, LearningState::None));

        self.epoch_info.writer = new_epoch;
        self.become_terminated(0);
    }

    pub fn handle_received(&mut self, target: &str, index: u32) {
        if let Some(progress) = self.copy_set.get_mut(target) {
            progress.on_received(index, 0);
        }
    }

    pub fn handle_timeout(&mut self, target: &str, range: Range<u32>, bytes: usize) {
        match &mut self.learning_state {
            LearningState::None | LearningState::Terminated => {
                if let Some(progress) = self.copy_set.get_mut(target) {
                    progress.on_timeout(range, bytes)
                }
            }
            LearningState::Sealing { pending, .. } => {
                // resend to target again.
                pending.insert(target.to_owned());
            }
            LearningState::Learning { pending, .. } => {
                if range.end < range.start {
                    // It means timeout by learning resend to target again.
                    pending.insert(target.to_owned());
                } else {
                    // otherwise timeout by replicating
                    if let Some(progress) = self.copy_set.get_mut(target) {
                        progress.on_timeout(range, bytes)
                    }
                }
            }
        }
    }

    pub fn handle_sealed(&mut self, target: &str, acked_index: u32) {
        if self.sealed_set.contains(target) {
            return;
        }

        self.sealed_set.insert(target.into());
        if let LearningState::Sealing { acked_indexes, .. } = &mut self.learning_state {
            acked_indexes.push(acked_index);
            if let Some(actual_acked_index) = self
                .policy
                .actual_acked_index(self.copy_set.len(), acked_indexes)
            {
                // We have received satisfied SEALED response, now changes state to learn
                // pending entries.
                self.become_learning(actual_acked_index);
            }
        }
    }

    pub fn handle_learned(&mut self, target: &str, entries: Vec<(u32, Entry)>) {
        if let LearningState::Learning {
            group_reader,
            actual_acked_index,
            ..
        } = &mut self.learning_state
        {
            group_reader.append(target, entries);
            loop {
                if let Some((index, mut entry)) = group_reader.take_next_entry() {
                    if !matches!(entry, Entry::Bridge { .. }) {
                        // We must read the last one of acked entry to make sure it's not a bridge.
                        if *actual_acked_index < index {
                            entry.set_epoch(self.epoch_info.writer);
                            self.mem_store.append(entry);
                        }
                        continue;
                    }

                    let actual_acked_index = *actual_acked_index;
                    self.become_terminated(actual_acked_index);
                }
                break;
            }
        }
    }

    fn become_terminated(&mut self, actual_acked_index: u32) {
        // The next index is the `actual_acked_index`, means that all entries in all
        // stores are acked and a bridge record exists.  We could skip append new bridge
        // record in that case.
        if actual_acked_index < self.mem_store.next_index() {
            self.mem_store.append(self.epoch_info.bridge());
        }
        self.learning_state = LearningState::Terminated;
    }

    fn become_learning(&mut self, actual_acked_index: u32) {
        let group_reader = self.policy.new_group_reader(
            self.epoch_info.writer,
            actual_acked_index,
            self.copy_set.keys().cloned().collect(),
        );
        self.learning_state = LearningState::Learning {
            actual_acked_index,
            group_reader,
            pending: self.sealed_set.clone(),
        };

        // All progress has already received all acked entries.
        for progress in self.copy_set.values_mut() {
            progress.on_received(actual_acked_index, actual_acked_index);
        }
        // FIXME(w41ter) update entries epoch.
        self.mem_store = MemStore::recovery(self.epoch_info.writer, actual_acked_index + 1);
    }
}

struct EpochInfo {
    segment: u32,
    writer: u32,
}

impl EpochInfo {
    #[inline(always)]
    fn build_write(&self, target: String, write: Write) -> Mutate {
        Mutate {
            target,
            seg_epoch: self.segment,
            writer_epoch: self.writer,
            kind: MutKind::Write(write),
        }
    }

    #[inline(always)]
    fn build_seal(&self, target: String) -> Mutate {
        Mutate {
            target,
            seg_epoch: self.segment,
            writer_epoch: self.writer,
            kind: MutKind::Seal,
        }
    }

    #[inline(always)]
    fn build_learn(&self, target: String, start_index: u32) -> Learn {
        Learn {
            target,
            seg_epoch: self.segment,
            writer_epoch: self.writer,
            start_index,
        }
    }

    #[inline(always)]
    fn bridge(&self) -> Entry {
        Entry::Bridge { epoch: self.writer }
    }
}
