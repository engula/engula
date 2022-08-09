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

use std::{collections::HashMap, sync::Arc};

use engula_api::server::v1::*;
use tracing::{debug, error};

use crate::schedule::{
    actions::{Action, AddLearners, CreateReplicas, RemoveLearners, ReplaceVoters},
    provider::GroupProviders,
    scheduler::ScheduleContext,
    task::{Task, TaskState},
    tasks::{ActionTask, CURE_GROUP_TASK_ID},
};

pub struct CureGroup {
    providers: Arc<GroupProviders>,
}

impl CureGroup {
    pub fn new(providers: Arc<GroupProviders>) -> CureGroup {
        CureGroup { providers }
    }

    async fn replace_voters(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        learners: HashMap<u64, ReplicaDesc>,
        voters: HashMap<u64, ReplicaDesc>,
    ) {
        let replace_voters_action: Box<dyn Action> = Box::new(ReplaceVoters {
            providers: self.providers.clone(),
            incoming_voters: learners.values().cloned().collect(),
            demoting_voters: voters.values().cloned().collect(),
        });
        let remove_learners_action: Box<dyn Action> = Box::new(RemoveLearners {
            providers: self.providers.clone(),
            learners: learners.values().cloned().collect(),
        });
        let task: Box<dyn Task> = Box::new(ActionTask::new(
            ctx.next_task_id(),
            vec![replace_voters_action, remove_learners_action],
        ));
        ctx.delegate(task);
    }

    async fn cure_group(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        voters: HashMap<u64, ReplicaDesc>,
    ) {
        let incoming_voters = match self
            .alloc_addition_replicas(ctx, "cure-group", voters.len())
            .await
        {
            Some(voters) => voters,
            None => return,
        };
        let create_replicas_action: Box<dyn Action> = Box::new(CreateReplicas {
            replicas: incoming_voters.clone(),
        });
        let add_learners_action: Box<dyn Action> = Box::new(AddLearners {
            providers: self.providers.clone(),
            learners: incoming_voters.clone(),
        });
        let replace_voters_action: Box<dyn Action> = Box::new(ReplaceVoters {
            providers: self.providers.clone(),
            incoming_voters,
            demoting_voters: voters.values().cloned().collect(),
        });
        let remove_learners_action: Box<dyn Action> = Box::new(RemoveLearners {
            providers: self.providers.clone(),
            learners: voters.values().cloned().collect(),
        });
        let task: Box<dyn Task> = Box::new(ActionTask::new(
            ctx.next_task_id(),
            vec![
                create_replicas_action,
                add_learners_action,
                replace_voters_action,
                remove_learners_action,
            ],
        ));
        ctx.delegate(task);
    }

    async fn remove_learners(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        learners: HashMap<u64, ReplicaDesc>,
    ) {
        let action: Box<dyn Action> = Box::new(RemoveLearners {
            providers: self.providers.clone(),
            learners: learners.values().cloned().collect(),
        });
        let task: Box<dyn Task> = Box::new(ActionTask::new(ctx.next_task_id(), vec![action]));
        ctx.delegate(task);
    }

    /// Alloc addition replicas from root.
    async fn alloc_addition_replicas(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        who: &str,
        num_required: usize,
    ) -> Option<Vec<ReplicaDesc>> {
        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;
        let req = AllocReplicaRequest {
            group_id,
            epoch: ctx.replica.epoch(),
            current_term: ctx.current_term,
            leader_id: replica_id,
            num_required: num_required as u64,
        };
        match ctx.provider.root_client.alloc_replica(req).await {
            Ok(resp) => Some(resp.replicas),
            Err(
                e @ (engula_client::Error::ResourceExhausted(_)
                | engula_client::Error::EpochNotMatch(_)),
            ) => {
                debug!(
                    "group {group_id} replica {replica_id} alloc addition replicas for {who}: {e}",
                );
                None
            }
            Err(e) => {
                error!(
                    "group {group_id} replica {replica_id} alloc addition replicas for {who}: {e}",
                );
                None
            }
        }
    }
}

#[crate::async_trait]
impl Task for CureGroup {
    fn id(&self) -> u64 {
        CURE_GROUP_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        let mut online_learners = HashMap::default();
        let mut offline_learners = HashMap::default();
        let mut offline_voters = HashMap::default();

        let replicas = self.providers.descriptor.replicas();
        let lost_peers = self.providers.raft_state.lost_peers();
        for r in &replicas {
            match ReplicaRole::from_i32(r.role).unwrap() {
                ReplicaRole::Voter | ReplicaRole::IncomingVoter => {
                    if lost_peers.contains(&r.id) {
                        offline_voters.insert(r.id, r.clone());
                    }
                }
                ReplicaRole::Learner => {
                    if lost_peers.contains(&r.id) {
                        offline_learners.insert(r.id, r.clone());
                    } else {
                        online_learners.insert(r.id, r.clone());
                    }
                }
                _ => {}
            }
        }

        if !offline_voters.is_empty() && !online_learners.is_empty() {
            self.replace_voters(ctx, online_learners, offline_voters)
                .await;
            return TaskState::Pending(None);
        }

        if !offline_voters.is_empty() {
            self.cure_group(ctx, offline_voters).await;
            return TaskState::Pending(None);
        }

        if !offline_learners.is_empty() {
            self.remove_learners(ctx, offline_learners).await;
            return TaskState::Pending(None);
        }

        if !online_learners.is_empty() {
            self.remove_learners(ctx, online_learners).await;
            return TaskState::Pending(None);
        }

        // TODO(walter) remove voters if there exists some voters.

        TaskState::Pending(None)
    }
}
