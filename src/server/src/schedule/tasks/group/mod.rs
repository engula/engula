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

mod cure_group;
mod descriptor;
mod orphan_replica;
mod promote;
mod raft_state;
mod replica_states;

use std::collections::HashMap;

pub use self::{
    cure_group::CureGroup, descriptor::WatchGroupDescriptor, orphan_replica::RemoveOrphanReplica,
    promote::PromoteGroup, raft_state::WatchRaftState, replica_states::WatchReplicaStates,
};
use super::ActionTask;
use crate::schedule::{
    scheduler::ScheduleContext,
    task::{Task, TaskState},
};

#[derive(Default)]
pub struct GroupLockTable {
    config_change: Option</* task_id */ u64>,
    locked_replicas: HashMap</* replica_id */ u64, /* task_id */ u64>,
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
    pub fn new() -> Self {
        GroupLockTable::default()
    }

    #[inline]
    pub fn is_replica_locked(&self, replica_id: u64) -> bool {
        self.locked_replicas.contains_key(&replica_id)
    }

    pub fn lock(&mut self, task_id: u64, replicas: &[u64]) -> Option<GroupLocks> {
        if replicas
            .iter()
            .any(|r| self.locked_replicas.contains_key(r))
        {
            None
        } else {
            for r in replicas {
                self.locked_replicas.insert(*r, task_id);
            }
            Some(GroupLocks {
                replicas: replicas.to_owned(),
            })
        }
    }

    pub fn config_change(&mut self, task_id: u64, replicas: &[u64]) -> Option<GroupLocks> {
        if self.config_change.is_some() {
            None
        } else if let Some(locks) = self.lock(task_id, replicas) {
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
        if matches!(self.config_change, Some(v) if v == task_id) {
            self.config_change = None;
        }
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
