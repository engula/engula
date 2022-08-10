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

use std::{collections::HashMap, sync::Arc, time::Duration};

use engula_api::server::v1::*;
use tracing::{debug, error};

use super::ActionTaskWithLocks;
use crate::schedule::{
    actions::{AddLearners, CreateReplicas, RemoveLearners, ReplaceVoters},
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
        peers: Vec<u64>,
        learners: HashMap<u64, ReplicaDesc>,
        voters: HashMap<u64, ReplicaDesc>,
    ) {
        let task_id = ctx.next_task_id();
        let locks = ctx
            .group_lock_table
            .config_change(task_id, &peers)
            .expect("Check conflicts in before steps");
        let learners = learners.values().cloned().collect::<Vec<_>>();
        let voters = voters.values().cloned().collect::<Vec<_>>();
        let replace_voters_action = Box::new(ReplaceVoters {
            providers: self.providers.clone(),
            incoming_voters: learners,
            demoting_voters: voters.clone(),
        });
        let remove_learners_action = Box::new(RemoveLearners {
            providers: self.providers.clone(),
            learners: voters,
        });
        let action_task =
            ActionTask::new(task_id, vec![replace_voters_action, remove_learners_action]);
        ctx.delegate(Box::new(ActionTaskWithLocks::new(locks, action_task)));
    }

    async fn cure_group(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        mut peers: Vec<u64>,
        voters: HashMap<u64, ReplicaDesc>,
    ) {
        let incoming_voters = match self
            .alloc_addition_replicas(ctx, "cure-group", voters.len())
            .await
        {
            Some(voters) => voters,
            None => return,
        };
        peers.extend(incoming_voters.iter().map(|r| r.id));
        let task_id = ctx.next_task_id();
        let locks = ctx
            .group_lock_table
            .config_change(task_id, &peers)
            .expect("Check conflicts in before steps");
        let create_replicas_action = Box::new(CreateReplicas {
            replicas: incoming_voters.clone(),
        });
        let add_learners_action = Box::new(AddLearners {
            providers: self.providers.clone(),
            learners: incoming_voters.clone(),
        });
        let replace_voters_action = Box::new(ReplaceVoters {
            providers: self.providers.clone(),
            incoming_voters,
            demoting_voters: voters.values().cloned().collect(),
        });
        let remove_learners_action = Box::new(RemoveLearners {
            providers: self.providers.clone(),
            learners: voters.values().cloned().collect(),
        });
        let action_task = ActionTask::new(
            ctx.next_task_id(),
            vec![
                create_replicas_action,
                add_learners_action,
                replace_voters_action,
                remove_learners_action,
            ],
        );
        ctx.delegate(Box::new(ActionTaskWithLocks::new(locks, action_task)));
    }

    async fn remove_learners(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        peers: Vec<u64>,
        learners: HashMap<u64, ReplicaDesc>,
    ) {
        let task_id = ctx.next_task_id();
        let locks = ctx
            .group_lock_table
            .config_change(task_id, &peers)
            .expect("Check conflicts in before steps");
        let action_task = ActionTask::new(
            task_id,
            vec![Box::new(RemoveLearners {
                providers: self.providers.clone(),
                learners: learners.values().cloned().collect(),
            })],
        );
        ctx.delegate(Box::new(ActionTaskWithLocks::new(locks, action_task)));
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
        if ctx.group_lock_table.has_config_change() {
            tracing::debug!(
                "group {} replica {} cure group task pending: has config change",
                ctx.group_id,
                ctx.replica_id
            );
            return TaskState::Pending(Some(Duration::from_secs(1)));
        }

        let mut peers = vec![];
        let mut online_learners = HashMap::default();
        let mut offline_learners = HashMap::default();
        let mut offline_voters = HashMap::default();

        let replicas = self.providers.descriptor.replicas();
        let lost_peers = self.providers.raft_state.lost_peers();
        for r in &replicas {
            if ctx.group_lock_table.is_replica_locked(r.id) {
                tracing::debug!(
                    "group {} replica {} cure group task pending: replica {} is locked",
                    ctx.group_id,
                    ctx.replica_id,
                    r.id
                );
                return TaskState::Pending(Some(Duration::from_secs(1)));
            }

            peers.push(r.id);
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
            self.replace_voters(ctx, peers, online_learners, offline_voters)
                .await;
        } else if !offline_voters.is_empty() {
            self.cure_group(ctx, peers, offline_voters).await;
        } else if !offline_learners.is_empty() {
            self.remove_learners(ctx, peers, offline_learners).await;
        } else if !online_learners.is_empty() {
            self.remove_learners(ctx, peers, online_learners).await;
        } else {
            tracing::debug!(
                "group {} replica {} cure group task pending: {} online learners, {} offline learners {} offline voters",
                ctx.group_id,
                ctx.replica_id,
                online_learners.len(),
                offline_learners.len(),
                offline_voters.len(),
            );
            return TaskState::Pending(Some(Duration::from_secs(1)));
        }

        // TODO(walter) remove voters if there exists some voters.
        tracing::debug!(
            "group {} replica {} cure group task pending: submit new tasks",
            ctx.group_id,
            ctx.replica_id,
        );

        TaskState::Pending(Some(Duration::from_secs(10)))
    }
}
