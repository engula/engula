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
    sync::{Arc, Mutex},
    task::Waker,
};

use engula_api::server::v1::{GroupDesc, MigrationDesc, RaftRole, ReplicaDesc, ReplicaState};
use futures::channel::mpsc;
use tracing::info;

use super::{fsm::StateMachineObserver, ReplicaInfo};
use crate::{node::job::StateChannel, raftgroup::StateObserver, serverpb::v1::MigrationState};

pub struct LeaseState {
    pub leader_id: u64,
    /// the largest term which state machine already known.
    pub applied_term: u64,
    pub replica_state: ReplicaState,
    pub descriptor: GroupDesc,
    pub migration_state: Option<MigrationState>,
    pub migration_state_subscriber: mpsc::UnboundedSender<MigrationState>,
    pub leader_subscribers: Vec<Waker>,
}

/// A struct that observes changes to `GroupDesc` and `ReplicaState` , and broadcasts those changes
/// while saving them to `LeaseState`.
#[derive(Clone)]
pub struct LeaseStateObserver {
    info: Arc<ReplicaInfo>,
    lease_state: Arc<Mutex<LeaseState>>,
    state_channel: StateChannel,
}

impl LeaseState {
    pub fn new(
        descriptor: GroupDesc,
        migration_state: Option<MigrationState>,
        migration_state_subscriber: mpsc::UnboundedSender<MigrationState>,
    ) -> Self {
        LeaseState {
            descriptor,
            migration_state,
            migration_state_subscriber,
            leader_id: 0,
            applied_term: 0,
            replica_state: ReplicaState::default(),
            leader_subscribers: vec![],
        }
    }

    #[inline]
    pub fn is_raft_leader(&self) -> bool {
        self.replica_state.role == RaftRole::Leader as i32
    }

    /// At least one log for the current term has been applied?
    #[inline]
    pub fn is_log_term_matched(&self) -> bool {
        self.applied_term == self.replica_state.term
    }

    #[inline]
    pub fn is_ready_for_serving(&self) -> bool {
        self.is_raft_leader() && self.is_log_term_matched()
    }

    #[inline]
    pub fn is_migrating(&self) -> bool {
        self.migration_state.is_some()
    }

    #[inline]
    pub fn is_migrating_shard(&self, shard_id: u64) -> bool {
        self.migration_state
            .as_ref()
            .map(|s| s.get_shard_id() == shard_id)
            .unwrap_or_default()
    }

    #[inline]
    pub fn is_same_migration(&self, desc: &MigrationDesc) -> bool {
        self.migration_state.as_ref().unwrap().get_migration_desc() == desc
    }

    #[inline]
    pub fn wake_all_waiters(&mut self) {
        for waker in std::mem::take(&mut self.leader_subscribers) {
            waker.wake();
        }
    }

    #[inline]
    pub fn leader_descriptor(&self) -> Option<ReplicaDesc> {
        self.descriptor
            .replicas
            .iter()
            .find(|r| r.id == self.leader_id)
            .cloned()
    }

    #[inline]
    pub fn terminate(&mut self) {
        self.wake_all_waiters();
        self.migration_state_subscriber.close_channel();
    }
}

impl LeaseStateObserver {
    pub fn new(
        info: Arc<ReplicaInfo>,
        lease_state: Arc<Mutex<LeaseState>>,
        state_channel: StateChannel,
    ) -> Self {
        LeaseStateObserver {
            info,
            lease_state,
            state_channel,
        }
    }

    fn update_replica_state(
        &self,
        leader_id: u64,
        voted_for: u64,
        term: u64,
        role: RaftRole,
    ) -> (ReplicaState, Option<GroupDesc>) {
        let replica_state = ReplicaState {
            replica_id: self.info.replica_id,
            group_id: self.info.group_id,
            term,
            voted_for,
            role: role.into(),
            node_id: self.info.node_id,
        };
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.leader_id = leader_id;
        lease_state.replica_state = replica_state.clone();
        let desc = if role == RaftRole::Leader {
            info!(
                "replica {} become leader of group {} at term {}",
                self.info.replica_id, self.info.group_id, term
            );
            Some(lease_state.descriptor.clone())
        } else {
            None
        };
        (replica_state, desc)
    }

    fn update_descriptor(&self, descriptor: GroupDesc) -> bool {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.descriptor = descriptor;
        lease_state.replica_state.role == RaftRole::Leader as i32
    }
}

impl StateObserver for LeaseStateObserver {
    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole) {
        let (state, desc) = self.update_replica_state(leader_id, voted_for, term, role);
        self.state_channel
            .broadcast_replica_state(self.info.group_id, state);
        if let Some(desc) = desc {
            self.state_channel
                .broadcast_group_descriptor(self.info.group_id, desc);
        }
    }
}

impl StateMachineObserver for LeaseStateObserver {
    fn on_descriptor_updated(&mut self, descriptor: GroupDesc) {
        if self.update_descriptor(descriptor.clone()) {
            self.state_channel
                .broadcast_group_descriptor(self.info.group_id, descriptor);
        }
    }

    fn on_term_updated(&mut self, term: u64) {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.applied_term = term;
        if lease_state.is_ready_for_serving() {
            info!(
                "replica {} is ready for serving requests of group {} at term {}",
                self.info.replica_id, self.info.group_id, term
            );
            lease_state.wake_all_waiters();
            if let Some(migration_state) = lease_state.migration_state.as_ref() {
                lease_state
                    .migration_state_subscriber
                    .unbounded_send(migration_state.to_owned())
                    .unwrap_or_default();
            }
        }
    }

    fn on_migrate_state_updated(&mut self, migration_state: Option<MigrationState>) {
        let mut lease_state = self.lease_state.lock().unwrap();
        lease_state.migration_state = migration_state;
        if let Some(migration_state) = lease_state.migration_state.as_ref() {
            if lease_state.is_ready_for_serving() {
                lease_state
                    .migration_state_subscriber
                    .unbounded_send(migration_state.to_owned())
                    .unwrap_or_default();
            }
        }
    }
}
