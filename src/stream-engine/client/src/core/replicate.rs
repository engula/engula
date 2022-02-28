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
    replicate_policy: ReplicatePolicy,
    mem_store: MemStore,
    copy_set: HashMap<String, Progress>,

    acked_seq: Sequence,
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
            replicate_policy: policy,
            mem_store: MemStore::new(epoch),
            copy_set: copy_set
                .into_iter()
                .map(|c| (c, Progress::new(epoch)))
                .collect(),
            acked_seq: Sequence::default(),
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

    #[inline(always)]
    pub fn acked_seq(&self) -> Sequence {
        self.acked_seq
    }

    #[inline(always)]
    pub fn append(&mut self, entry: Entry) -> Sequence {
        self.mem_store.append(entry)
    }

    #[inline]
    pub fn on_tick(&mut self) {
        self.copy_set.values_mut().for_each(Progress::on_tick);
    }

    pub fn all_target_matched(&self) -> bool {
        let last_index = self.mem_store.next_index().saturating_sub(1);
        matches!(self.learning_state, LearningState::Terminated)
            && self.copy_set.iter().all(|(_, p)| p.is_matched(last_index))
    }

    pub fn advance_acked_sequence(&mut self) -> bool {
        let acked_seq = self
            .replicate_policy
            .advance_acked_sequence(self.epoch_info.segment, &self.copy_set);
        if self.acked_seq < acked_seq {
            self.acked_seq = acked_seq;
            true
        } else {
            false
        }
    }

    fn replicate(&mut self) -> Vec<Mutate> {
        let mut pending_writes = vec![];
        for (server_id, progress) in &mut self.copy_set {
            let next_index = self.mem_store.next_index();
            let (Range { start, mut end }, quota) = progress.next_chunk(next_index);
            let (acked_seq, entries, bytes) = if quota > 0
                && start == next_index
                && !progress.is_replicating_acked_seq(self.acked_seq)
            {
                // All entries are replicated, might broadcast acked sequence.
                (self.acked_seq, vec![], 0)
            } else if let Some((entries, bytes)) = self.mem_store.range(start..end, quota) {
                // Do not forward acked sequence to unmatched index.
                end = start + entries.len() as u32;
                let matched_acked_seq =
                    Sequence::min(self.acked_seq, self.epoch_info.sequence(end - 1));
                (matched_acked_seq, entries, bytes)
            } else {
                continue;
            };

            progress.replicate(end, bytes, acked_seq.index);
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

    pub fn broadcast(&mut self) -> (Vec<Mutate>, Vec<Learn>) {
        let mut pending_writes = vec![];
        let mut pending_learns = vec![];

        let mut replicable = false;
        match &mut self.learning_state {
            LearningState::None | LearningState::Terminated => {
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
            let mut writes = self.replicate();
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
            progress.on_received(index);
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
                .replicate_policy
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
        let group_reader = self.replicate_policy.new_group_reader(
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
            progress.on_received(actual_acked_index);
        }
        self.acked_seq = Sequence::new(self.epoch_info.segment, actual_acked_index);
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

    #[inline(always)]
    fn sequence(&self, index: u32) -> Sequence {
        Sequence::new(self.segment, index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(epoch: u32, range: Range<u32>) -> Vec<Entry> {
        range
            .into_iter()
            .map(|i| i.to_le_bytes())
            .map(|bytes| Entry::Event {
                epoch,
                event: bytes.into(),
            })
            .collect()
    }

    fn append_entries(rep: &mut Replicate, entries: Vec<Entry>) {
        for entry in entries {
            rep.append(entry);
        }
    }

    fn receive_writes(rep: &mut Replicate, mutates: Vec<Mutate>) {
        for m in mutates {
            match m.kind {
                MutKind::Write(write) if !write.entries.is_empty() => {
                    rep.handle_received(&m.target, write.range.end - 1);
                }
                _ => {}
            }
        }
    }

    fn advance_and_receive_all_writes(rep: &mut Replicate) {
        loop {
            let pending_writes = rep.replicate();
            if pending_writes.is_empty() {
                break;
            }
            receive_writes(rep, pending_writes);
        }
    }

    #[test]
    fn replicate_keep_continuously() {
        let mut rep = Replicate::new(1, 1, vec!["a".to_owned()], ReplicatePolicy::Simple);
        let entries = make_entries(1, 1..1024);
        append_entries(&mut rep, entries.clone());

        let mut rep_entries = vec![];
        let mut next_index = 1;
        loop {
            let pending_writes = rep.replicate();
            if pending_writes.is_empty() {
                break;
            }
            for m in pending_writes {
                match m.kind {
                    MutKind::Write(mut write) => {
                        assert_eq!(write.range.start, next_index);
                        next_index = write.range.end;
                        rep_entries.append(&mut write.entries);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            }
        }

        assert_eq!(rep_entries, entries);
    }

    #[test]
    fn replicate_acked_index_if_all_entries_are_replicated() {
        let mut rep = Replicate::new(1, 1, vec!["a".to_owned()], ReplicatePolicy::Simple);
        let entries = make_entries(1, 1..1024);
        append_entries(&mut rep, entries);

        advance_and_receive_all_writes(&mut rep);
        rep.advance_acked_sequence();

        assert_eq!(rep.acked_seq(), Sequence::new(1, 1023));

        let pending_writes = rep.replicate();
        assert_eq!(pending_writes.len(), 1);
        assert!(
            matches!(&pending_writes[0].kind, MutKind::Write(write) if write.entries.is_empty() && write.acked_seq.index == 1023)
        );
    }

    fn append_and_replicate_entries(rep: &mut Replicate, epoch: u32, range: Range<u32>) {
        append_entries(rep, make_entries(epoch, range));
        loop {
            let pending_writes = rep.replicate();
            if pending_writes.is_empty() {
                break;
            }
        }
    }

    #[test]
    fn do_not_replicate_acked_index_if_congested() {
        let mut rep = Replicate::new(1, 1, vec!["a".to_owned()], ReplicatePolicy::Simple);

        let entries = make_entries(1, 0..1024);
        append_entries(&mut rep, entries);
        advance_and_receive_all_writes(&mut rep);

        append_and_replicate_entries(&mut rep, 1, 1024..2048);
        append_and_replicate_entries(&mut rep, 1, 2048..4096);
        // Since the target has received entries before 2048, the replicate should
        // replicate acked index to stores immediately.
        rep.handle_received("a", 2047);
        // But the transport is congest...
        rep.handle_timeout("a", 2048..4096, 1234);

        let mutates = rep.replicate();
        assert_eq!(mutates.len(), 0);
    }

    // left test cases
    // 2. the recovering replicas still need to advance. Specially fast
    //    recovery need execute advance too.
    // 3. replicating from latest acked index
    // 4. if latest acked index is bridge, that means recovered.
}
