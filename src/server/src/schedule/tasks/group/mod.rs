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

mod durable;
mod migration;
mod orphan_replica;
mod promote;
mod watch_descriptor;
mod watch_raft_state;
mod watch_replica_states;

use std::collections::HashMap;

use engula_api::server::v1::{ReplicaDesc, ScheduleState};

pub use self::{
    durable::DurableGroup, migration::ReplicaMigration, orphan_replica::RemoveOrphanReplica,
    promote::PromoteGroup, watch_descriptor::WatchGroupDescriptor,
    watch_raft_state::WatchRaftState, watch_replica_states::WatchReplicaStates,
};
use super::ActionTask;
use crate::schedule::{
    scheduler::ScheduleContext,
    task::{Task, TaskState},
};

pub struct GroupLockTable {
    group_id: u64,
    config_change: Option</* task_id */ u64>,
    locked_replicas: HashMap</* replica_id */ u64, /* task_id */ u64>,

    states_updated: bool,
    states: HashMap<u64, ScheduleState>,
}

#[derive(Default)]
pub struct GroupLocks {
    replicas: Vec<u64>,
}

pub struct ActionTaskWithLocks {
    inner: ActionTask,
    locks: GroupLocks,
}

impl GroupLockTable {
    pub fn new(group_id: u64) -> Self {
        GroupLockTable {
            group_id,
            config_change: None,
            locked_replicas: HashMap::new(),
            states_updated: false,
            states: HashMap::new(),
        }
    }

    #[inline]
    pub fn is_replica_locked(&self, replica_id: u64) -> bool {
        self.locked_replicas.contains_key(&replica_id)
    }

    pub fn lock(
        &mut self,
        task_id: u64,
        replicas: &[u64],
        incoming_replicas: &[ReplicaDesc],
        outgoing_replicas: &[ReplicaDesc],
    ) -> Option<GroupLocks> {
        if replicas
            .iter()
            .any(|r| self.locked_replicas.contains_key(r))
        {
            None
        } else {
            for r in replicas {
                self.locked_replicas.insert(*r, task_id);
            }
            let state = ScheduleState {
                group_id: 0,
                incoming_replicas: incoming_replicas.to_owned(),
                outgoing_replicas: outgoing_replicas.to_owned(),
            };
            self.states_updated = true;
            self.states.insert(task_id, state);
            Some(GroupLocks {
                replicas: replicas.to_owned(),
            })
        }
    }

    pub fn config_change(
        &mut self,
        task_id: u64,
        replicas: &[u64],
        incoming_replicas: &[ReplicaDesc],
        outgoing_replicas: &[ReplicaDesc],
    ) -> Option<GroupLocks> {
        if self.config_change.is_some() {
            None
        } else if let Some(locks) =
            self.lock(task_id, replicas, incoming_replicas, outgoing_replicas)
        {
            self.config_change = Some(task_id);
            Some(locks)
        } else {
            None
        }
    }

    #[inline]
    pub fn has_config_change(&self) -> bool {
        self.config_change.is_some()
    }

    pub fn release(&mut self, task_id: u64, locked: GroupLocks) {
        for r in locked.replicas {
            self.locked_replicas.remove(&r);
        }
        if self.states.remove(&task_id).is_some() {
            self.states_updated = true;
        }
        if matches!(self.config_change, Some(v) if v == task_id) {
            self.config_change = None;
        }
    }

    pub fn take_updated_states(&mut self) -> Option<ScheduleState> {
        if !self.states_updated {
            return None;
        }

        self.states_updated = false;
        let mut accumulated_state = ScheduleState {
            group_id: self.group_id,
            incoming_replicas: vec![],
            outgoing_replicas: vec![],
        };
        for state in self.states.values() {
            accumulated_state
                .incoming_replicas
                .extend(state.incoming_replicas.iter().cloned());
            accumulated_state
                .outgoing_replicas
                .extend(state.outgoing_replicas.iter().cloned());
        }
        Some(accumulated_state)
    }
}

impl ActionTaskWithLocks {
    pub fn new(locks: GroupLocks, inner: ActionTask) -> Self {
        ActionTaskWithLocks { inner, locks }
    }
}

#[crate::async_trait]
impl Task for ActionTaskWithLocks {
    #[inline]
    fn id(&self) -> u64 {
        self.inner.id()
    }

    #[inline]
    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        let state = self.inner.poll(ctx).await;
        if matches!(state, TaskState::Terminated) {
            ctx.group_lock_table
                .release(self.inner.id(), std::mem::take(&mut self.locks));
        }
        state
    }
}
