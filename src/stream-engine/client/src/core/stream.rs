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

use bitflags::bitflags;
use log::{error, info, warn};
use stream_engine_proto::ObserverState;

use super::{message::*, Replicate, INITIAL_EPOCH};
use crate::{policy::Policy as ReplicatePolicy, Entry, EpochState, Error, Result, Role, Sequence};

#[derive(Default)]
pub(super) struct Ready {
    pub acked_seq: Sequence,

    pub pending_writes: Vec<Mutate>,
    pub pending_learns: Vec<Learn>,
}

bitflags! {
    struct Flags : u64 {
        const NONE = 0;
        const ACK_ADVANCED = 0x1;
    }
}

pub(super) struct StreamStateMachine {
    pub stream_id: u64,
    pub epoch: u32,
    pub role: Role,
    pub leader: String,
    pub state: ObserverState,
    pub replicate_policy: ReplicatePolicy,

    pub acked_seq: Sequence,

    latest_tick: usize,

    replicate: Box<Replicate>,
    recovering_replicates: HashMap<u32, Box<Replicate>>,

    ready: Ready,

    flags: Flags,
}

impl Display for StreamStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream {} epoch {}", self.stream_id, self.epoch)
    }
}

#[allow(dead_code)]
impl StreamStateMachine {
    pub fn new(stream_id: u64) -> Self {
        StreamStateMachine {
            stream_id,
            epoch: INITIAL_EPOCH,
            role: Role::Follower,
            leader: "".to_owned(),
            state: ObserverState::Following,
            latest_tick: 0,
            replicate_policy: ReplicatePolicy::Simple,
            acked_seq: Sequence::default(),
            replicate: Box::new(Replicate::new(
                INITIAL_EPOCH,
                INITIAL_EPOCH,
                vec![],
                ReplicatePolicy::Simple,
            )),
            ready: Ready::default(),
            flags: Flags::NONE,
            // pending_epochs: Vec::default(),
            recovering_replicates: HashMap::new(),
        }
    }

    pub fn epoch_state(&self) -> EpochState {
        EpochState {
            epoch: self.epoch as u64,
            role: self.role,
            leader: if self.epoch == INITIAL_EPOCH {
                None
            } else {
                Some(self.leader.clone())
            },
        }
    }

    pub fn tick(&mut self) {
        self.latest_tick += 1;
    }

    pub fn promote(
        &mut self,
        epoch: u32,
        role: Role,
        leader: String,
        copy_set: Vec<String>,
        pending_epochs: Vec<(u32, Vec<String>)>,
    ) -> bool {
        if self.epoch >= epoch {
            warn!(
                "stream {} epoch {} reject staled promote, epoch: {}, role: {:?}, leader: {}",
                self.stream_id, self.epoch, epoch, role, leader
            );
            return false;
        }

        let prev_epoch = std::mem::replace(&mut self.epoch, epoch);
        let prev_role = self.role;
        self.leader = leader;
        self.role = role;
        self.state = match role {
            Role::Leader => ObserverState::Leading,
            Role::Follower => ObserverState::Following,
        };
        if self.role == Role::Leader {
            let new_replicate = Box::new(Replicate::new(
                self.epoch,
                self.epoch,
                copy_set,
                self.replicate_policy,
            ));
            if self.replicate.epoch() + 1 == self.epoch && prev_role == self.role {
                // do fast recovery
                debug_assert_eq!(pending_epochs.len(), 1);
                debug_assert_eq!(pending_epochs[0].0, prev_epoch);

                // TODO(w41ter) when we recovery, what happen if the previous epoch is still
                // recovery?.
                debug_assert!(self.recovering_replicates.is_empty());
                let mut prev_replicate = std::mem::replace(&mut self.replicate, new_replicate);
                prev_replicate.become_recovery(self.epoch);
                self.recovering_replicates
                    .insert(prev_epoch, prev_replicate);
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
                                self.epoch,
                                copy_set,
                                self.replicate_policy,
                            )),
                        )
                    })
                    .collect();
                self.replicate = new_replicate;
            }
            debug_assert!(self.recovering_replicates.len() <= 2);
        }

        info!(
            "stream {} promote epoch from {} to {}, new role {:?}, leader {}, num copy {}",
            self.stream_id,
            prev_epoch,
            epoch,
            self.role,
            self.leader,
            self.replicate.copy_set.len(),
        );

        true
    }

    pub fn step(&mut self, msg: Message) {
        use std::cmp::Ordering;
        match msg.epoch.cmp(&self.epoch) {
            Ordering::Less => {
                // FIXME(w41ter) When fast recovery is executed, the in-flights messages's epoch
                // might be staled.
                warn!(
                    "{} ignore staled msg {} from {}, epoch {}",
                    self, msg.detail, msg.target, msg.epoch
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
            MsgDetail::Received { index } => {
                self.handle_received(&msg.target, msg.seg_epoch, index)
            }
            MsgDetail::Recovered => self.handle_recovered(msg.seg_epoch),
            MsgDetail::Timeout { range, bytes } => {
                self.handle_timeout(&msg.target, msg.seg_epoch, range, bytes)
            }
            MsgDetail::Learned(learned) => {
                self.handle_learned(&msg.target, msg.seg_epoch, learned.entries)
            }
            MsgDetail::Sealed { acked_index } => {
                self.handle_sealed(&msg.target, msg.seg_epoch, acked_index)
            }
            MsgDetail::Rejected => {}
        }
    }

    pub fn propose(&mut self, event: Box<[u8]>) -> Result<Sequence> {
        if self.role == Role::Follower {
            Err(Error::NotLeader(self.leader.clone()))
        } else {
            let entry = Entry::Event {
                epoch: self.epoch,
                event,
            };
            Ok(self.replicate.mem_store.append(entry))
        }
    }

    pub fn collect(&mut self) -> Option<Ready> {
        if self.role == Role::Leader {
            self.advance();
            self.all_replicates_broadcast();
            self.flags = Flags::NONE;
            self.ready.acked_seq = self.acked_seq;
            Some(std::mem::take(&mut self.ready))
        } else {
            None
        }
    }

    fn advance(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);
        // Don't ack any entries if there exists a pending segment.
        if !self.recovering_replicates.is_empty() {
            return;
        }

        let acked_seq = self
            .replicate_policy
            .advance_acked_sequence(self.epoch, &self.replicate.copy_set);
        if self.acked_seq < acked_seq {
            self.acked_seq = acked_seq;
            self.flags |= Flags::ACK_ADVANCED;
        }
    }

    fn broadcast(
        ready: &mut Ready,
        replicate: &mut Replicate,
        latest_tick: usize,
        acked_seq: Sequence,
        acked_index_advanced: bool,
    ) {
        let (mut writes, mut learns) =
            replicate.broadcast(latest_tick, acked_seq, acked_index_advanced);
        ready.pending_writes.append(&mut writes);
        ready.pending_learns.append(&mut learns);
    }

    fn all_replicates_broadcast(&mut self) {
        debug_assert_eq!(self.role, Role::Leader);

        if let Some(epoch) = self.recovering_replicates.keys().min().cloned() {
            let replicate = self.recovering_replicates.get_mut(&epoch).unwrap();
            Self::broadcast(
                &mut self.ready,
                replicate,
                self.latest_tick,
                self.acked_seq,
                false,
            );
        }

        // Do not replicate entries if there exists two pending segments.
        if self.recovering_replicates.len() == 2 {
            return;
        }

        Self::broadcast(
            &mut self.ready,
            &mut self.replicate,
            self.latest_tick,
            self.acked_seq,
            self.flags.contains(Flags::ACK_ADVANCED),
        );
    }

    fn handle_received(&mut self, target: &str, epoch: u32, index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        if self.epoch == epoch {
            self.replicate.handle_received(target, index);
        } else if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_received(target, index);
            if replicate.all_target_matched() {
                // FIXME(w41ter) Shall I use a special value to identify master sealing
                // operations?
                self.ready.pending_writes.push(Mutate {
                    target: "<MASTER>".into(),
                    seg_epoch: epoch,
                    writer_epoch: self.epoch,
                    kind: MutKind::Seal,
                });
            }
        } else {
            warn!(
                "{} receive staled RECEIVED from {}, epoch {}",
                self, target, epoch
            );
        }
    }

    fn handle_timeout(&mut self, target: &str, epoch: u32, range: Range<u32>, bytes: usize) {
        debug_assert_eq!(self.role, Role::Leader);
        if self.epoch == epoch {
            self.replicate.handle_timeout(target, range, bytes);
        } else if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_timeout(target, range, bytes);
        } else {
            warn!(
                "{} receive staled TIMEOUT from {}, epoch {}",
                self, target, epoch
            );
        }
    }

    fn handle_learned(&mut self, target: &str, epoch: u32, entries: Vec<(u32, Entry)>) {
        debug_assert_eq!(self.role, Role::Leader);
        debug_assert_ne!(self.epoch, epoch, "current epoch don't need to recovery");
        if let Some(replicate) = self.recovering_replicates.get_mut(&epoch) {
            replicate.handle_learned(target, entries);
        } else {
            warn!("{} receive staled LEARNED of epoch {}", self, epoch);
        }
    }

    fn handle_sealed(&mut self, target: &str, epoch: u32, acked_index: u32) {
        debug_assert_eq!(self.role, Role::Leader);
        debug_assert_ne!(self.epoch, epoch, "current epoch don't need to recovery");
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
