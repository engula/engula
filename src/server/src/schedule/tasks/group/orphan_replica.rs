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
    sync::Arc,
    time::{Duration, Instant},
};

use engula_api::server::v1::*;
use tracing::info;

use super::ActionTaskWithLocks;
use crate::schedule::{
    actions::{ClearReplicaState, RemoveReplica},
    event_source::EventSource,
    provider::GroupProviders,
    scheduler::ScheduleContext,
    task::{Task, TaskState},
    tasks::{ActionTask, REMOVE_ORPHAN_REPLICA_TASK_ID},
};

pub struct RemoveOrphanReplica {
    orphan_replicas: HashMap<u64, Instant>,
    providers: Arc<GroupProviders>,
}

impl RemoveOrphanReplica {
    pub(crate) fn new(providers: Arc<GroupProviders>) -> RemoveOrphanReplica {
        RemoveOrphanReplica {
            orphan_replicas: HashMap::default(),
            providers,
        }
    }

    async fn dismiss_orphan_replica(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        replica: ReplicaDesc,
        group: GroupDesc,
    ) {
        let target_id = replica.id;
        let task_id = ctx.next_task_id();
        if let Some(locks) =
            ctx.group_lock_table
                .lock(task_id, group.epoch, &[target_id], &[], &[replica.clone()])
        {
            let action_task = ActionTask::new(
                task_id,
                vec![
                    Box::new(RemoveReplica::new(group, replica)),
                    Box::new(ClearReplicaState::new(target_id)),
                ],
            );
            ctx.delegate(Box::new(ActionTaskWithLocks::new(locks, action_task)));
            self.orphan_replicas.remove(&target_id);
        }
    }

    fn apply_states(&mut self, replica_states: &[ReplicaState], desc: &GroupDesc) {
        let replica_set = desc.replicas.iter().map(|r| r.id).collect::<HashSet<_>>();
        self.orphan_replicas.retain(|k, _| !replica_set.contains(k));
        for r in replica_states {
            if !replica_set.contains(&r.replica_id) {
                self.orphan_replicas
                    .entry(r.replica_id)
                    .or_insert_with(Instant::now);
            }
        }
    }

    fn get_dismiss_replicas(&self, ctx: &mut ScheduleContext<'_>) -> HashSet<u64> {
        if ctx
            .cfg
            .testing_knobs
            .disable_orphan_replica_detecting_intervals
        {
            self.orphan_replicas
                .keys()
                .cloned()
                .collect::<HashSet<u64>>()
        } else {
            let now = Instant::now();
            let interval = Duration::from_secs(60);
            self.orphan_replicas
                .iter()
                .filter(|(_, &instant)| instant + interval < now)
                .map(|(&id, _)| id)
                .collect::<HashSet<_>>()
        }
    }
}

#[crate::async_trait]
impl Task for RemoveOrphanReplica {
    fn id(&self) -> u64 {
        REMOVE_ORPHAN_REPLICA_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        let replica_states = self.providers.replica_states.replica_states();
        let desc = self.providers.descriptor.descriptor();
        if desc.replicas.is_empty() {
            self.providers.descriptor.watch(self.id());
            return TaskState::Pending(None);
        }

        self.apply_states(&replica_states, &desc);
        let dismiss_replicas = self.get_dismiss_replicas(ctx);
        for s in &replica_states {
            if !dismiss_replicas.contains(&s.replica_id)
                || ctx.group_lock_table.is_replica_locked(s.replica_id)
            {
                continue;
            }

            let replica_id = s.replica_id;
            let group_id = s.group_id;
            info!("group {group_id} find a orphan replica {replica_id}, try remove it",);

            let replica = ReplicaDesc {
                id: replica_id,
                node_id: s.node_id,
                ..Default::default()
            };
            self.dismiss_orphan_replica(ctx, replica, desc.clone())
                .await;
        }

        if ctx
            .cfg
            .testing_knobs
            .disable_orphan_replica_detecting_intervals
        {
            TaskState::Pending(Some(Duration::from_millis(1)))
        } else {
            TaskState::Pending(Some(Duration::from_secs(60)))
        }
    }
}
