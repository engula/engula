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

#[derive(Derivative)]
#[derivative(Debug)]
enum LearningState {
    None,
    Sealing {
        // Fast recovery means that the current leader already knows all pending entries and does
        // not need to go through the learning state.
        fast_recovery: bool,
        acked_indexes: Vec<u32>,
        /// Need send SEAL request to this target.
        pending: HashSet<String>,
    },
    Learning {
        actual_acked_index: u32,
        #[derivative(Debug = "ignore")]
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
                fast_recovery: false,
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
    pub fn writer_epoch(&self) -> u32 {
        self.epoch_info.writer
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

    pub fn is_enough_targets_acked(&self) -> bool {
        let last_index = self.mem_store.next_index().saturating_sub(1);
        matches!(self.learning_state, LearningState::Terminated)
            && self
                .replicate_policy
                .is_enough_targets_acked(last_index, &self.copy_set)
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
                group_reader,
                ..
            } => {
                // We would also replicate acked entries.
                replicable = true;
                for target in std::mem::take(pending) {
                    // Learn from the previous breakpoint. If no data has been read before, we need
                    // to start reading from acked_index, the reason is that we don't know whether a
                    // bridge entry has already been committed.
                    let start_index = group_reader.target_next_index(&target);
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

        let pending = self.copy_set.keys().cloned().collect();
        self.epoch_info.writer = new_epoch;
        self.learning_state = LearningState::Sealing {
            fast_recovery: true,
            acked_indexes: Vec::new(),
            pending,
        };
    }

    pub fn handle_received(&mut self, target: &str, matched_index: u32, acked_index: u32) {
        if let Some(progress) = self.copy_set.get_mut(target) {
            progress.on_received(matched_index, acked_index);
        }
    }

    pub fn handle_timeout(&mut self, target: &str, range: Option<Range<u32>>, bytes: usize) {
        match &mut self.learning_state {
            LearningState::None | LearningState::Terminated => {
                if let Some(range) = range {
                    if let Some(progress) = self.copy_set.get_mut(target) {
                        progress.on_timeout(range, bytes)
                    }
                }
            }
            LearningState::Sealing { pending, .. } => {
                // resend to target again.
                if range.is_none() {
                    pending.insert(target.to_owned());
                }
            }
            LearningState::Learning { pending, .. } => {
                if let Some(range) = range {
                    if let Some(progress) = self.copy_set.get_mut(target) {
                        progress.on_timeout(range, bytes)
                    }
                } else {
                    // It means timeout by learning resend to target again.
                    pending.insert(target.to_owned());
                }
            }
        }
    }

    pub fn handle_sealed(&mut self, target: &str, acked_index: u32) {
        if self.sealed_set.contains(target) {
            return;
        }

        self.sealed_set.insert(target.into());
        if let LearningState::Sealing {
            fast_recovery,
            acked_indexes,
            ..
        } = &mut self.learning_state
        {
            acked_indexes.push(acked_index);
            if let Some(actual_acked_index) = self
                .replicate_policy
                .actual_acked_index(self.copy_set.len(), acked_indexes)
            {
                // We have received satisfied SEALED response, now changes state to learn
                // pending entries or just terminated if do fast recovering.
                if *fast_recovery {
                    self.become_terminated(true, 0);
                } else {
                    self.become_learning(actual_acked_index);
                }
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
                    self.become_terminated(false, actual_acked_index);
                }
                break;
            }
        }
    }

    fn become_terminated(&mut self, fast_recovery: bool, actual_acked_index: u32) {
        // The next index is the `actual_acked_index`, means that all entries in all
        // stores are acked and a bridge record exists.  We could skip append new bridge
        // record in that case.
        //
        // For fast recovering, there should ensures that always append a bridge record
        // to mem store.
        if fast_recovery || actual_acked_index + 1 < self.mem_store.next_index() {
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
            progress.on_received(actual_acked_index, actual_acked_index);
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
pub fn make_learned_entries(epoch: u32, start: u32, end: u32) -> Vec<(u32, Entry)> {
    (start..end)
        .into_iter()
        .zip(self::tests::make_entries(epoch, start..end).into_iter())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn make_entries(epoch: u32, range: Range<u32>) -> Vec<Entry> {
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
                    rep.handle_received(&m.target, write.range.end - 1, write.range.start - 1);
                }
                _ => unreachable!("mutate is {:?}", m),
            }
        }
    }

    fn advance_and_receive_all_writes(rep: &mut Replicate) {
        loop {
            let (pending_writes, pending_learns) = rep.broadcast();
            assert!(pending_learns.is_empty());
            if pending_writes.is_empty() {
                break;
            }
            receive_writes(rep, pending_writes);
        }
    }

    fn advance_and_receive_sealed(rep: &mut Replicate, acked_indexes: HashMap<String, u32>) {
        let (pending_writes, pending_learns) = rep.broadcast();
        assert!(pending_learns.is_empty());
        for m in pending_writes {
            match m.kind {
                MutKind::Seal => {
                    rep.handle_sealed(
                        &m.target,
                        acked_indexes.get(&m.target).cloned().unwrap_or(0),
                    );
                }
                _ => unreachable!("mutate is {:?}", m),
            }
        }
    }

    /// If there is no error during the replicating process, the range of the
    /// output mutations should keep continuously.
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

    /// Once all entries are replicated and received by target, the acked index
    /// should be broadcast to target immediately to speed up reading process.
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

    /// The actual acked index of Learning state should obtained from sealed
    /// responses.
    #[test]
    fn recovery_acked_index_from_sealed_response() {
        let mut rep = Replicate::recovery(1, 2, vec!["a".to_owned()], ReplicatePolicy::Simple);
        assert_eq!(rep.acked_seq, Sequence::default());

        let mut acked_indexes: HashMap<String, u32> = HashMap::new();
        acked_indexes.insert("a".to_owned(), 1024u32);
        advance_and_receive_sealed(&mut rep, acked_indexes);
        assert!(matches!(rep.learning_state, LearningState::Learning { .. }));

        assert_eq!(rep.acked_seq, Sequence::new(1, 1024));
    }

    /// If an Replicate become recovering from normal state, the don't need to
    /// learn un-acked entries from stores because it already known all pending
    /// entries and the replicating progress of each one of copy set.
    #[test]
    fn fast_recovering_step_terminated_directly_from_recovering() {
        let mut rep = Replicate::new(1, 1, vec!["a".to_owned()], ReplicatePolicy::Simple);

        let entries = make_entries(1, 0..1024);
        append_entries(&mut rep, entries);
        advance_and_receive_all_writes(&mut rep);

        append_and_replicate_entries(&mut rep, 1, 1024..2048);
        rep.become_recovery(2);
        assert!(matches!(rep.learning_state, LearningState::Sealing { .. }));

        let mut acked_indexes = HashMap::new();
        acked_indexes.insert("a".to_owned(), 1024u32);
        advance_and_receive_sealed(&mut rep, acked_indexes);
        assert!(matches!(rep.learning_state, LearningState::Terminated));
    }

    /// Once a target of Replicate become congested, there no any message could
    /// be send even the message is only a acked write.
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
        rep.handle_received("a", 2047, 0);
        // But the transport is congest...
        rep.handle_timeout("a", Some(2048..4096), 1234);

        let mutates = rep.replicate();
        assert_eq!(mutates.len(), 0);
    }

    /// Like a normal state Replicate, recovering replicate also need to advance
    /// acked sequence manually.
    #[test]
    fn recovering_replicate_also_need_advance() {
        struct TestCase {
            recovery: bool,
        }
        let cases: Vec<TestCase> = vec![TestCase { recovery: false }, TestCase { recovery: true }];
        for case in cases {
            let mut rep = Replicate::new(1, 1, vec!["a".to_owned()], ReplicatePolicy::Simple);
            append_entries(&mut rep, make_entries(1, 1..1024));
            advance_and_receive_all_writes(&mut rep);
            rep.advance_acked_sequence();

            append_and_replicate_entries(&mut rep, 1, 1024..2048);

            let mut rep = if case.recovery {
                Replicate::recovery(1, 2, vec!["a".to_owned()], ReplicatePolicy::Simple)
            } else {
                rep.become_recovery(2);
                rep
            };

            assert!(matches!(rep.learning_state, LearningState::Sealing { .. }));

            let mut acked_indexes = HashMap::new();
            acked_indexes.insert("a".to_owned(), 1023);
            advance_and_receive_sealed(&mut rep, acked_indexes);

            if case.recovery {
                let (writes, learns) = rep.broadcast();
                assert!(writes.is_empty(), "writes is {:?}", writes);
                assert!(!learns.is_empty());
                rep.handle_learned("a", make_learned_entries(1, 1024, 2048));
                rep.handle_learned("a", vec![]); // Learning is finished.
            }
            assert!(
                matches!(rep.learning_state, LearningState::Terminated),
                "state is {:?}",
                rep.learning_state
            );
            advance_and_receive_all_writes(&mut rep);

            assert_eq!(rep.acked_seq, Sequence::new(1, 1023));
            rep.advance_acked_sequence();

            // Include bridge entries.
            assert_eq!(rep.acked_seq, Sequence::new(1, 2048));
        }
    }

    /// The fast recovering Replicate don't use the acked index from sealed
    /// responses, because it already know the actually replicating progresses.
    #[test]
    fn fast_recovering_replicate_start_from_latest_acked_index() {
        let mut rep = Replicate::new(1, 1, vec!["a".to_owned()], ReplicatePolicy::Simple);

        let entries = make_entries(1, 1..1024);
        append_entries(&mut rep, entries);
        advance_and_receive_all_writes(&mut rep);
        rep.advance_acked_sequence();
        assert_eq!(rep.acked_seq, Sequence::new(1, 1023));

        append_entries(&mut rep, make_entries(1, 1024..2048));
        advance_and_receive_all_writes(&mut rep);
        rep.advance_acked_sequence();
        assert_eq!(rep.acked_seq, Sequence::new(1, 2047));

        rep.become_recovery(2);
        assert!(matches!(rep.learning_state, LearningState::Sealing { .. }));
        assert_eq!(rep.acked_seq, Sequence::new(1, 2047));

        let mut acked_indexes = HashMap::new();
        acked_indexes.insert("a".to_owned(), 1024u32);
        advance_and_receive_sealed(&mut rep, acked_indexes);
        assert!(matches!(rep.learning_state, LearningState::Terminated));
        assert_eq!(rep.acked_seq, Sequence::new(1, 2047));
    }

    /// If a recovering process faulted after all entries acked (include bridge
    /// record), the new recovery Replicate would continue recovering without
    /// change the acked bridge record.
    #[test]
    fn recovering_from_acked_bridge_record() {
        let mut rep = Replicate::recovery(1, 2, vec!["a".to_owned()], ReplicatePolicy::Simple);
        let mut acked_indexes = HashMap::new();
        acked_indexes.insert("a".to_owned(), 100);
        advance_and_receive_sealed(&mut rep, acked_indexes);
        assert!(matches!(rep.learning_state, LearningState::Learning { .. }));
        let (writes, learns) = rep.broadcast();
        assert!(writes.is_empty());
        assert!(!learns.is_empty());
        rep.handle_learned("a", vec![(100, Entry::Bridge { epoch: 1 })]);
        rep.handle_learned("a", vec![]);
        assert!(matches!(rep.learning_state, LearningState::Terminated));
        assert_eq!(rep.mem_store.next_index(), 101);
        assert_eq!(rep.acked_seq, Sequence::new(1, 100));
    }

    #[test]
    fn sealing_timeout() {
        let mut rep = Replicate::recovery(1, 2, vec!["a".to_owned()], ReplicatePolicy::Simple);
        assert!(matches!(rep.learning_state, LearningState::Sealing { .. }));
        rep.broadcast();
        rep.broadcast();
        rep.handle_timeout("a", None, 0);
        assert!(matches!(rep.learning_state, LearningState::Sealing { .. }));

        let mut acked_indexes = HashMap::new();
        acked_indexes.insert("a".to_owned(), 100);
        advance_and_receive_sealed(&mut rep, acked_indexes);
        assert!(matches!(rep.learning_state, LearningState::Learning { .. }));
    }

    #[test]
    fn learning_timeout() {
        let mut rep = Replicate::recovery(1, 2, vec!["a".to_owned()], ReplicatePolicy::Simple);
        assert!(matches!(rep.learning_state, LearningState::Sealing { .. }));
        let mut acked_indexes = HashMap::new();
        acked_indexes.insert("a".to_owned(), 100);
        advance_and_receive_sealed(&mut rep, acked_indexes);
        assert!(matches!(rep.learning_state, LearningState::Learning { .. }));

        let (_, learns) = rep.broadcast();
        assert!(!learns.is_empty());
        let (_, learns) = rep.broadcast();
        assert!(learns.is_empty());

        rep.handle_learned("a", make_learned_entries(1, 100, 110));
        rep.handle_timeout("a", None, 0);
        let (_, learns) = rep.broadcast();
        assert!(!learns.is_empty());

        // start from previous breakpoint.
        assert!(learns
            .iter()
            .filter(|l| l.target == "a")
            .all(|l| { l.start_index == 110 }));
    }

    /// The epoch of learned entries should be updated to writer epoch, except
    /// fast recovering.
    #[test]
    fn recovering_will_replicate_entries_with_writer_epoch() {
        for recovery in &[false, true] {
            let mut rep = if *recovery {
                Replicate::recovery(1, 3, vec!["a".to_owned()], ReplicatePolicy::Simple)
            } else {
                let mut rep = Replicate::new(1, 1, vec!["a".to_owned()], ReplicatePolicy::Simple);
                append_and_replicate_entries(&mut rep, 1, 1..1024);
                rep.become_recovery(3);
                rep
            };

            let mut acked_indexes = HashMap::new();
            acked_indexes.insert("a".to_owned(), 100);
            advance_and_receive_sealed(&mut rep, acked_indexes);
            if *recovery {
                let (_, learns) = rep.broadcast();
                assert!(!learns.is_empty());
                rep.handle_learned("a", make_learned_entries(2, 100, 1024));
                rep.handle_learned("a", vec![]);
            }

            assert!(matches!(rep.learning_state, LearningState::Terminated));
            let (writes, _) = rep.broadcast();
            for w in writes {
                match w.kind {
                    MutKind::Write(write) => {
                        for entry in write.entries {
                            if *recovery {
                                assert!(entry.epoch() == 3);
                            } else {
                                assert!(entry.epoch() <= 3);
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
