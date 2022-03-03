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

use std::{collections::HashMap, fmt::Display, ops::Range};

use stream_engine_proto::ObserverState;
use tracing::{error, info, warn};

use super::{message::*, Replicate, INITIAL_EPOCH};
use crate::{policy::Policy as ReplicatePolicy, Entry, EpochState, Error, Result, Role, Sequence};

#[derive(Default)]
pub(crate) struct Ready {
    pub acked_seq: Sequence,

    pub pending_writes: Vec<Mutate>,
    pub pending_learns: Vec<Learn>,
    pub restored_segment: Option<Restored>,
}

pub(crate) struct StreamStateMachine {
    pub stream_id: u64,
    pub writer_epoch: u32,
    pub role: Role,
    pub leader: String,
    pub replicate_policy: ReplicatePolicy,

    replicate: Box<Replicate>,
    recovering_replicates: HashMap<u32, Box<Replicate>>,

    ready: Ready,
}

impl Display for StreamStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream {} epoch {}", self.stream_id, self.writer_epoch)
    }
}

#[allow(dead_code)]
impl StreamStateMachine {
    pub fn new(stream_id: u64) -> Self {
        StreamStateMachine {
            stream_id,
            writer_epoch: INITIAL_EPOCH,
            role: Role::Follower,
            leader: "".to_owned(),
            replicate_policy: ReplicatePolicy::Simple,
            replicate: Box::new(Replicate::new(
                INITIAL_EPOCH,
                INITIAL_EPOCH,
                vec![],
                ReplicatePolicy::Simple,
            )),
            ready: Ready::default(),
            recovering_replicates: HashMap::new(),
        }
    }

    pub fn epoch_state(&self) -> EpochState {
        EpochState {
            epoch: self.writer_epoch as u64,
            role: self.role,
            leader: if self.writer_epoch == INITIAL_EPOCH {
                None
            } else {
                Some(self.leader.clone())
            },
        }
    }

    #[inline(always)]
    pub fn acked_seq(&self) -> Sequence {
        self.replicate.acked_seq()
    }

    #[inline(always)]
    pub fn observer_state(&self) -> ObserverState {
        match self.role {
            Role::Leader if self.recovering_replicates.is_empty() => ObserverState::Leading,
            Role::Leader => ObserverState::Recovering,
            Role::Follower => ObserverState::Following,
        }
    }

    pub fn tick(&mut self) {
        self.replicate.on_tick();
        self.recovering_replicates
            .values_mut()
            .map(Box::as_mut)
            .for_each(Replicate::on_tick);
    }

    pub fn promote(
        &mut self,
        epoch: u32,
        role: Role,
        leader: String,
        copy_set: Vec<String>,
        pending_epochs: Vec<(u32, Vec<String>)>,
    ) -> bool {
        if self.writer_epoch >= epoch {
            warn!(
                "stream {} epoch {} reject staled promote, epoch: {}, role: {:?}, leader: {}",
                self.stream_id, self.writer_epoch, epoch, role, leader
            );
            return false;
        }

        let prev_epoch = std::mem::replace(&mut self.writer_epoch, epoch);
        let prev_role = self.role;
        self.leader = leader;
        self.role = role;

        let mut recovering_epochs = vec![];
        if self.role == Role::Leader {
            let new_replicate = Box::new(Replicate::new(
                self.writer_epoch,
                self.writer_epoch,
                copy_set,
                self.replicate_policy,
            ));
            if prev_epoch + 1 == self.writer_epoch && prev_role == self.role {
                // do fast recovery
                debug_assert_eq!(pending_epochs.len(), 1);
                debug_assert_eq!(pending_epochs[0].0, prev_epoch);

                // TODO(w41ter) when we recovery, what happen if the previous epoch is still
                // recovery?.
                debug_assert!(self.recovering_replicates.is_empty());
                let mut prev_replicate = std::mem::replace(&mut self.replicate, new_replicate);
                prev_replicate.become_recovery(self.writer_epoch);
                self.recovering_replicates
                    .insert(prev_epoch, prev_replicate);
                recovering_epochs.push(prev_epoch);
            } else {
                // Sort in reverse to ensure that the smallest is at the end. See
                // `StreamStateMachine::handle_recovered` for details.
                self.recovering_replicates = pending_epochs
                    .into_iter()
                    .map(|(epoch, copy_set)| {
                        (
                            epoch,
                            Box::new(Replicate::recovery(
                                epoch,
                                self.writer_epoch,
                                copy_set,
                                self.replicate_policy,
                            )),
                        )
                    })
                    .collect();
                self.replicate = new_replicate;
                recovering_epochs = self.recovering_replicates.keys().cloned().collect();
            }
            debug_assert!(self.recovering_replicates.len() <= 2);
        }

        info!(
            "stream {} promote epoch from {} to {}, new role {:?}, leader {}, recovery epochs {:?}",
            self.stream_id, prev_epoch, epoch, self.role, self.leader, recovering_epochs
        );

        true
    }

    pub fn step(&mut self, msg: Message) {
        use std::cmp::Ordering;
        match msg.writer_epoch.cmp(&self.writer_epoch) {
            // Allowing sealed responses makes the fast recovering process more flexible.
            Ordering::Less if !self.recovering_replicates.contains_key(&msg.segment_epoch) => {
                warn!(
                    "{} ignore staled msg {} from {}, epoch {}",
                    self, msg.detail, msg.target, msg.writer_epoch
                );
                return;
            }
            Ordering::Greater => {
                todo!("should promote itself epoch");
            }
            Ordering::Equal if self.role != Role::Leader => {
                error!("{} role {} receive {}", self, self.role, msg.detail);
                return;
            }
            _ => {}
        }

        match msg.detail {
            MsgDetail::Received {
                matched_index,
                acked_index,
            } => self.handle_received(&msg.target, msg.segment_epoch, matched_index, acked_index),
            MsgDetail::Recovered => self.handle_recovered(msg.segment_epoch),
            MsgDetail::Timeout { range, bytes } => {
                self.handle_timeout(&msg.target, msg.segment_epoch, range, bytes)
            }
            MsgDetail::Learned(learned) => {
                self.handle_learned(&msg.target, msg.segment_epoch, learned.entries)
            }
            MsgDetail::Sealed { acked_index } => {
                self.handle_sealed(&msg.target, msg.segment_epoch, acked_index)
            }
            MsgDetail::Rejected => {}
        }
    }

    pub fn propose(&mut self, event: Box<[u8]>) -> Result<Sequence> {
        if self.role == Role::Follower {
            Err(Error::NotLeader(self.leader.clone()))
        } else {
            let entry = Entry::Event {
                epoch: self.writer_epoch,
                event,
            };
            Ok(self.replicate.append(entry))
        }
    }

    pub fn collect(&mut self) -> Option<Ready> {
        if self.role == Role::Leader {
            self.advance();
            self.all_replicates_broadcast();
            self.ready.acked_seq = self.replicate.acked_seq();
            Some(std::mem::take(&mut self.ready))
        } else {
            None
        }
    }

    fn advance(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);

        // Don't ack any entries if there exists a pending segment.
        if !self.recovering_replicates.is_empty() {
            for replicate in self.recovering_replicates.values_mut() {
                replicate.advance_acked_sequence();
            }
        } else {
            self.replicate.advance_acked_sequence();
        }
    }

    fn broadcast(ready: &mut Ready, replicate: &mut Replicate) {
        let (mut writes, mut learns) = replicate.broadcast();
        ready.pending_writes.append(&mut writes);
        ready.pending_learns.append(&mut learns);
    }

    fn all_replicates_broadcast(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);

        if let Some(epoch) = self.recovering_replicates.keys().min().cloned() {
            let replicate = self.recovering_replicates.get_mut(&epoch).unwrap();
            Self::broadcast(&mut self.ready, replicate);
        }

        // Do not replicate entries if there exists two pending segments.
        if self.recovering_replicates.len() != 2 {
            Self::broadcast(&mut self.ready, &mut self.replicate);
        }
    }

    fn handle_received(
        &mut self,
        target: &str,
        segment_epoch: u32,
        matched_index: u32,
        acked_index: u32,
    ) {
        debug_assert_eq!(self.role, Role::Leader);
        if self.writer_epoch == segment_epoch {
            self.replicate
                .handle_received(target, matched_index, acked_index);
        } else if let Some(replicate) = self.recovering_replicates.get_mut(&segment_epoch) {
            replicate.handle_received(target, matched_index, acked_index);
            Self::try_to_finish_recovering(self.stream_id, &mut self.ready, replicate);
        } else {
            warn!(
                "{} receive staled RECEIVED from {}, epoch {}",
                self, target, segment_epoch
            );
        }
    }

    fn handle_timeout(
        &mut self,
        target: &str,
        epoch: u32,
        range: Option<Range<u32>>,
        bytes: usize,
    ) {
        debug_assert_eq!(self.role, Role::Leader);
        if self.writer_epoch == epoch {
            self.replicate.handle_timeout(target, range, bytes);
        } else if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_timeout(target, range, bytes);
            Self::try_to_finish_recovering(self.stream_id, &mut self.ready, replicate);
        } else {
            warn!(
                "{} receive staled TIMEOUT from {}, epoch {}",
                self, target, epoch
            );
        }
    }

    fn handle_learned(&mut self, target: &str, epoch: u32, entries: Vec<(u32, Entry)>) {
        debug_assert_eq!(self.role, Role::Leader);
        debug_assert_ne!(
            self.writer_epoch, epoch,
            "current epoch don't need to recovery"
        );
        if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_learned(target, entries);
        } else {
            warn!("{} receive staled LEARNED of epoch {}", self, epoch);
        }
    }

    fn handle_sealed(&mut self, target: &str, epoch: u32, acked_index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        debug_assert_ne!(
            self.writer_epoch, epoch,
            "current epoch don't need to recovery"
        );
        if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_sealed(target, acked_index);
        } else {
            warn!("{} receive staled SEALED of epoch {}", self, epoch);
        }
    }

    fn handle_recovered(&mut self, seg_epoch: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        info!(
            "{} receive {}, seg epoch: {}, num pending epochs {}",
            self,
            MsgDetail::Recovered,
            seg_epoch,
            self.recovering_replicates.len()
        );

        self.recovering_replicates.remove(&seg_epoch);
    }

    fn try_to_finish_recovering(stream_id: u64, ready: &mut Ready, replicate: &Replicate) {
        if ready.restored_segment.is_none() && replicate.is_enough_targets_acked() {
            info!(
                "stream {} segment {} is recovered, now make it sealed",
                stream_id,
                replicate.epoch()
            );
            // FIXME(w41ter) too many sealing request.
            ready.restored_segment = Some(Restored {
                segment_epoch: replicate.epoch(),
                writer_epoch: replicate.writer_epoch(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle_recovered(sm: &mut StreamStateMachine, seg_epoch: u32) {
        sm.step(Message {
            target: "self".into(),
            segment_epoch: seg_epoch,
            writer_epoch: sm.writer_epoch,
            detail: MsgDetail::Recovered,
        });
    }

    fn receive_seals(sm: &mut StreamStateMachine, mutates: &Vec<Mutate>) {
        for mutate in mutates {
            if let MutKind::Seal = &mutate.kind {
                sm.step(Message {
                    target: mutate.target.clone(),
                    segment_epoch: mutate.seg_epoch,
                    writer_epoch: mutate.writer_epoch,
                    detail: MsgDetail::Sealed { acked_index: 0 },
                });
            }
        }
    }

    fn receive_writes(sm: &mut StreamStateMachine, mutates: &Vec<Mutate>) {
        for mutate in mutates {
            if let MutKind::Write(write) = &mutate.kind {
                let index = write.range.end - 1;
                sm.step(Message {
                    target: mutate.target.clone(),
                    segment_epoch: mutate.seg_epoch,
                    writer_epoch: mutate.writer_epoch,
                    detail: MsgDetail::Received {
                        matched_index: index,
                        acked_index: write.acked_seq.index,
                    },
                });
            }
        }
    }

    #[test]
    fn only_leader_receives_proposal() {
        let mut sm = StreamStateMachine::new(1);
        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Follower);

        match sm.propose(Box::new([0u8])) {
            Err(Error::NotLeader(_)) => {}
            _ => panic!("follower do not receive proposal"),
        }

        sm.promote(
            state.epoch as u32 + 1,
            Role::Leader,
            "self".into(),
            vec![],
            vec![],
        );

        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Leader);

        match sm.propose(Box::new([0u8])) {
            Ok(_) => {}
            _ => panic!("leader must receive proposal"),
        }
    }

    #[test]
    fn reject_staled_promote_request() {
        let mut sm = StreamStateMachine::new(1);
        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Follower);

        let target_epoch = state.epoch + 2;
        assert!(sm.promote(
            target_epoch as u32,
            Role::Leader,
            "self".into(),
            vec![],
            vec![],
        ));

        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.epoch, target_epoch);

        assert!(!sm.promote(
            (target_epoch - 1) as u32,
            Role::Leader,
            "self".into(),
            vec![],
            vec![]
        ));

        let state = sm.epoch_state();
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.epoch, target_epoch);
    }

    #[test]
    fn blocking_advance_until_all_previous_are_acked() {
        let mut sm = StreamStateMachine::new(1);

        let epoch = 10;
        let copy_set = vec!["a".to_string(), "b".to_string()];
        sm.promote(
            epoch,
            Role::Leader,
            "".to_string(),
            copy_set,
            vec![(9, vec![])],
        );
        sm.propose([0u8].into()).unwrap();
        sm.propose([1u8].into()).unwrap();
        sm.propose([2u8].into()).unwrap();

        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        receive_writes(&mut sm, &ready.as_ref().unwrap().pending_writes);

        // All entries are replicated.
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        // All segment are recovered.
        handle_recovered(&mut sm, 9);
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq >= Sequence::new(10, 3));
    }

    #[test]
    fn blocking_replication_if_exists_two_pending_segments() {
        let mut sm = StreamStateMachine::new(1);

        let epoch = 10;
        let copy_set = vec!["a".to_string(), "b".to_string()];
        sm.promote(
            epoch,
            Role::Leader,
            "".to_string(),
            copy_set,
            vec![(8, vec![]), (9, vec![])],
        );
        sm.propose([0u8].into()).unwrap();
        sm.propose([1u8].into()).unwrap();
        sm.propose([2u8].into()).unwrap();

        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        if !ready.as_ref().unwrap().pending_writes.is_empty() {
            panic!("Do not replicate entries if there exists two pending segments");
        }

        // All entries are replicated.
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        // A segment is recovered.
        handle_recovered(&mut sm, 8);
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq <= Sequence::new(10, 0));

        receive_writes(&mut sm, &ready.as_ref().unwrap().pending_writes);

        // All segment are recovered.
        handle_recovered(&mut sm, 9);
        let ready = sm.collect();
        assert!(ready.is_some());
        assert!(ready.as_ref().unwrap().acked_seq >= Sequence::new(10, 3));
    }

    /// When a stream promote and do fast recovering, the messages send in
    /// previous epoch should be accepted to advance the progresses.
    #[test]
    fn fast_recovering_would_receive_staled_response() {
        let mut sm = StreamStateMachine::new(1);
        let copy_set = vec!["a".to_string()];
        sm.promote(1, Role::Leader, "".to_string(), copy_set.clone(), vec![]);

        sm.propose([0u8].into()).unwrap();
        sm.propose([1u8].into()).unwrap();
        sm.propose([2u8].into()).unwrap();
        let ready = sm.collect();
        assert!(ready.is_some());

        let epoch = 2;
        sm.promote(
            epoch,
            Role::Leader,
            "".to_string(),
            copy_set.clone(),
            vec![(1, copy_set)],
        );

        let ready = ready.unwrap();
        receive_writes(&mut sm, &ready.pending_writes);

        let ready = sm.collect();
        assert!(ready.is_some());
        let ready = ready.unwrap();
        receive_seals(&mut sm, &ready.pending_writes);

        let ready = sm.collect();
        assert!(ready.is_some());
        let ready = ready.unwrap();
        for m in ready.pending_writes {
            match m.kind {
                MutKind::Write(w) if w.entries.len() == 1 => {},
                _ =>  panic!("Once the staled responses are accepted, there no any writes except acked writing or bridge record, write is {:?}", m),
            }
        }
    }

    #[test]
    fn restored_timeout() {
        let mut sm = StreamStateMachine::new(1);
        let copy_set = vec!["a".to_string()];
        sm.promote(1, Role::Leader, "".to_string(), copy_set.clone(), vec![]);

        sm.propose([0u8].into()).unwrap();
        sm.propose([1u8].into()).unwrap();
        sm.propose([2u8].into()).unwrap();
        let prev_ready = sm.collect();

        let epoch = 2;
        sm.promote(
            epoch,
            Role::Leader,
            "".to_string(),
            copy_set.clone(),
            vec![(1, copy_set)],
        );

        let ready = sm.collect();
        assert!(ready.is_some());
        let ready = ready.unwrap();
        receive_seals(&mut sm, &ready.pending_writes);

        let ready = prev_ready.unwrap();
        receive_writes(&mut sm, &ready.pending_writes);

        // advanced and broadcast bridge record.
        let ready = sm.collect().unwrap();
        receive_writes(&mut sm, &ready.pending_writes);

        // advanced and broadcast acked writes
        let ready = sm.collect().unwrap();
        receive_writes(&mut sm, &ready.pending_writes);

        let ready = sm.collect().unwrap();
        assert!(ready.restored_segment.is_some());

        sm.step(Message {
            target: "a".to_owned(),
            segment_epoch: 1,
            writer_epoch: 2,
            detail: MsgDetail::Timeout {
                range: None,
                bytes: 0,
            },
        });
        let ready = sm.collect().unwrap();
        assert!(ready.restored_segment.is_some());
    }

    fn advance_until_acked_seq(
        sm: &mut StreamStateMachine,
        expected_seq: Sequence,
        stores: HashMap<u32, Vec<(u32, Entry)>>,
    ) {
        loop {
            if let Some(ready) = sm.collect() {
                if ready.acked_seq >= expected_seq {
                    break;
                }

                if let Some(restored) = ready.restored_segment {
                    sm.step(Message {
                        target: Default::default(),
                        segment_epoch: restored.segment_epoch,
                        writer_epoch: restored.writer_epoch,
                        detail: MsgDetail::Recovered,
                    });
                }

                receive_writes(sm, &ready.pending_writes);
                receive_seals(sm, &ready.pending_writes);
                for learn in ready.pending_learns {
                    sm.step(Message {
                        target: learn.target.clone(),
                        segment_epoch: learn.seg_epoch,
                        writer_epoch: learn.writer_epoch,
                        detail: MsgDetail::Learned(Learned {
                            entries: stores.get(&learn.seg_epoch).cloned().unwrap(),
                        }),
                    });
                    sm.step(Message {
                        target: learn.target,
                        segment_epoch: learn.seg_epoch,
                        writer_epoch: learn.writer_epoch,
                        detail: MsgDetail::Learned(Learned { entries: vec![] }),
                    });
                }
            }
        }
    }

    #[test]
    fn completely_recovery_process() {
        use super::super::replicate::make_learned_entries;

        let mut stores = HashMap::new();
        stores.insert(8, make_learned_entries(8, 1, 1024));
        stores.insert(9, make_learned_entries(9, 1, 1024));

        let mut sm = StreamStateMachine::new(1);
        let copy_set = vec!["a".to_string()];

        let epoch = 10;
        let recovering_segments = vec![(8, copy_set.clone()), (9, copy_set.clone())];
        sm.promote(
            epoch,
            Role::Leader,
            "".to_string(),
            copy_set,
            recovering_segments,
        );

        let expected_seq = sm.propose([0u8].into()).unwrap();
        advance_until_acked_seq(&mut sm, expected_seq, stores);
    }
}
