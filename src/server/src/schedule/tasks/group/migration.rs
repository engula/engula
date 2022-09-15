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

use std::sync::Arc;

use tracing::{debug, info};

use super::ActionTaskWithLocks;
use crate::{
    schedule::{
        actions::*,
        event_source::EventSource,
        provider::GroupProviders,
        scheduler::ScheduleContext,
        task::{Task, TaskState},
        tasks::{ActionTask, REPLICA_MIGRATION_TASK_ID},
    },
    Error,
};

pub struct ReplicaMigration {
    providers: Arc<GroupProviders>,
}

impl ReplicaMigration {
    pub fn new(providers: Arc<GroupProviders>) -> Self {
        ReplicaMigration { providers }
    }
}

#[crate::async_trait]
impl Task for ReplicaMigration {
    fn id(&self) -> u64 {
        REPLICA_MIGRATION_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        if let Some(move_replicas) = self.providers.move_replicas.take() {
            let replicas = self.providers.descriptor.replicas();
            let mut peers = replicas.iter().map(|v| v.id).collect::<Vec<_>>();
            peers.extend(move_replicas.incoming_replicas.iter().map(|v| v.id));
            let task_id = ctx.next_task_id();
            // TODO: verify task pre-conditions.
            if let Some(locks) = ctx.group_lock_table.config_change(
                task_id,
                move_replicas.epoch,
                &peers,
                &move_replicas.incoming_replicas,
                &[],
            ) {
                let create_replicas_action =
                    CreateReplicas::new(move_replicas.incoming_replicas.clone());
                let add_learners_action = AddLearners {
                    providers: self.providers.clone(),
                    learners: move_replicas.incoming_replicas.clone(),
                };
                let replace_voters_action = ReplaceVoters {
                    providers: self.providers.clone(),
                    incoming_voters: move_replicas.incoming_replicas.clone(),
                    demoting_voters: move_replicas.outgoing_replicas.clone(),
                };
                let remove_learners_action = RemoveLearners {
                    providers: self.providers.clone(),
                    learners: move_replicas.outgoing_replicas.clone(),
                };
                let action_task = ActionTask::new(
                    task_id,
                    vec![
                        Box::new(create_replicas_action),
                        Box::new(add_learners_action),
                        Box::new(replace_voters_action),
                        Box::new(remove_learners_action),
                    ],
                );
                ctx.delegate(Box::new(ActionTaskWithLocks::new(locks, action_task)));
                move_replicas.sender.send(Ok(())).unwrap_or_default();
            } else {
                debug!("group {} replica {} reject moving replicas requests because config change already exists",
                    ctx.group_id, ctx.replica_id);
                move_replicas
                    .sender
                    .send(Err(Error::AlreadyExists("config change".to_owned())))
                    .unwrap_or_default();
            }
        }
        info!(
            "wait next migrate replica task, {}, {}",
            ctx.group_id, ctx.replica_id,
        );
        self.providers.move_replicas.watch(self.id());
        TaskState::Pending(None)
    }
}
