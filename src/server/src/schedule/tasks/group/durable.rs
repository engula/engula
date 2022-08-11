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
use tracing::{debug, error, info};

use super::ActionTaskWithLocks;
use crate::schedule::{
    actions::{AddLearners, CreateReplicas, RemoveLearners, ReplaceVoters},
    event_source::EventSource,
    provider::GroupProviders,
    scheduler::ScheduleContext,
    task::{Task, TaskState},
    tasks::{ActionTask, CURE_GROUP_TASK_ID},
};

#[derive(Default, Debug)]
struct ReplicaStats {
    peers: Vec<u64>,
    online_voters: HashMap<u64, ReplicaDesc>,
    offline_voters: HashMap<u64, ReplicaDesc>,
    online_learners: HashMap<u64, ReplicaDesc>,
    offline_learners: HashMap<u64, ReplicaDesc>,
}

pub struct DurableGroup {
    providers: Arc<GroupProviders>,
}

impl DurableGroup {
    pub fn new(providers: Arc<GroupProviders>) -> DurableGroup {
        DurableGroup { providers }
    }

    async fn replace_voters(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        peers: Vec<u64>,
        learners: HashMap<u64, ReplicaDesc>,
        voters: HashMap<u64, ReplicaDesc>,
    ) {
        let task_id = ctx.next_task_id();
        info!(
            "group {} replica {} task {task_id} replace voters {:?} with {:?}",
            ctx.group_id,
            ctx.replica_id,
            voters.keys(),
            learners.keys()
        );
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
        incoming_voters: Vec<ReplicaDesc>,
        outgoing_voters: HashMap<u64, ReplicaDesc>,
    ) {
        peers.extend(incoming_voters.iter().map(|r| r.id));
        let task_id = ctx.next_task_id();
        info!(
            "group {} replica {} task {task_id} add voters {:?} and remove voters {:?}",
            ctx.group_id,
            ctx.replica_id,
            incoming_voters.iter().map(|r| r.id),
            outgoing_voters.keys()
        );
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
            demoting_voters: outgoing_voters.values().cloned().collect(),
        });
        let remove_learners_action = Box::new(RemoveLearners {
            providers: self.providers.clone(),
            learners: outgoing_voters.values().cloned().collect(),
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
        info!(
            "group {} replica {} task {task_id} remove learners {:?}",
            ctx.group_id,
            ctx.replica_id,
            learners.keys()
        );
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

    async fn handle_replica_stats(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        stats: ReplicaStats,
    ) -> TaskState {
        let num_required = 3;
        self.providers.descriptor.watch(self.id());

        // Offline learners have no use value, remove them to simplify the logic.
        if !stats.offline_learners.is_empty() {
            self.remove_learners(ctx, stats.peers, stats.offline_learners)
                .await;
            return TaskState::Pending(Some(Duration::from_secs(30)));
        }

        // The redundant replicas can be deleted, and the offline ones will be deleted first, and
        // then the online ones will be considered.
        let total_voters = stats.online_voters.len() + stats.offline_voters.len();
        if total_voters > num_required {
            let exceeds = total_voters - num_required;
            let mut outgoing_voters = stats
                .offline_voters
                .into_iter()
                .take(exceeds)
                .collect::<HashMap<_, _>>();
            if outgoing_voters.len() < exceeds {
                self.select_dismiss_voters(
                    ctx,
                    stats.online_voters,
                    exceeds - outgoing_voters.len(),
                    &mut outgoing_voters,
                );
            }

            self.replace_voters(ctx, stats.peers, HashMap::default(), outgoing_voters)
                .await;
            return TaskState::Pending(Some(Duration::from_secs(30)));
        }

        // Since the redundant replicas have been removed, there can only be an equal or
        // insufficient number of replicas. If there are enough learners, it can directly promote
        // the learners to voter and replace offline voters. If there are not enough learners, it
        // can only apply to the root and add them into cluster.
        if stats.online_voters.len() < num_required {
            let acquires = num_required - stats.online_voters.len();
            if !stats.online_learners.is_empty() {
                let learners = stats
                    .online_learners
                    .into_iter()
                    .take(acquires)
                    .collect::<HashMap<_, _>>();
                let outgoing_voters = stats
                    .offline_voters
                    .into_iter()
                    .take(learners.len())
                    .collect::<HashMap<_, _>>();
                self.replace_voters(ctx, stats.peers, learners, outgoing_voters)
                    .await;
                return TaskState::Pending(Some(Duration::from_secs(30)));
            } else if let Some(incoming_voters) = self
                .alloc_addition_replicas(ctx, "cure-group", acquires)
                .await
            {
                self.cure_group(ctx, stats.peers, incoming_voters, stats.offline_voters)
                    .await;
                return TaskState::Pending(Some(Duration::from_secs(30)));
            } else {
                return TaskState::Pending(Some(Duration::from_secs(3)));
            }
        }

        // Now, online voters meet the requirements, and there are no offline voters, just delete
        // redundant learners.
        if !stats.online_learners.is_empty() {
            debug_assert!(stats.offline_voters.is_empty());
            debug_assert!(stats.offline_learners.is_empty());
            debug_assert_eq!(stats.online_voters.len(), num_required);
            self.remove_learners(ctx, stats.peers, stats.offline_learners)
                .await;
            return TaskState::Pending(Some(Duration::from_secs(30)));
        }

        TaskState::Pending(Some(Duration::from_secs(1)))
    }

    fn select_dismiss_voters(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        online_voters: HashMap<u64, ReplicaDesc>,
        num_required: usize,
        outgoing_voters: &mut HashMap<u64, ReplicaDesc>,
    ) {
        debug_assert!(num_required < online_voters.len());
        let mut matched_indexes = self.providers.raft_state.matched_indexes();
        matched_indexes.retain(|id, _| online_voters.contains_key(id));
        matched_indexes.remove(&ctx.replica_id);
        let mut matched_indexes = matched_indexes.into_iter().collect::<Vec<_>>();
        matched_indexes.sort_unstable_by_key(|&(_, index)| index);
        outgoing_voters.extend(
            matched_indexes
                .into_iter()
                .take(num_required)
                .map(|(id, _)| {
                    (
                        id,
                        online_voters
                            .get(&id)
                            .expect("matched_indexes only contains online voters")
                            .clone(),
                    )
                }),
        );
    }
}

#[crate::async_trait]
impl Task for DurableGroup {
    fn id(&self) -> u64 {
        CURE_GROUP_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        let replicas = self.providers.descriptor.replicas();
        if replicas.len() <= 1 || ctx.group_lock_table.has_config_change() {
            return TaskState::Pending(Some(Duration::from_secs(1)));
        }

        let lost_peers = self.providers.raft_state.lost_peers();
        let mut stats = ReplicaStats::default();
        for r in &replicas {
            if ctx.group_lock_table.is_replica_locked(r.id) {
                return TaskState::Pending(Some(Duration::from_secs(1)));
            }

            stats.peers.push(r.id);
            match ReplicaRole::from_i32(r.role).unwrap() {
                ReplicaRole::IncomingVoter | ReplicaRole::DemotingVoter => {
                    // in joint config change.
                    return TaskState::Pending(Some(Duration::from_secs(1)));
                }
                ReplicaRole::Voter => {
                    if lost_peers.contains(&r.id) {
                        stats.offline_voters.insert(r.id, r.clone());
                    } else {
                        stats.online_voters.insert(r.id, r.clone());
                    }
                }
                ReplicaRole::Learner => {
                    if lost_peers.contains(&r.id) {
                        stats.offline_learners.insert(r.id, r.clone());
                    } else {
                        stats.online_learners.insert(r.id, r.clone());
                    }
                }
            }
        }

        info!("replica stats: {:?}", stats);
        self.handle_replica_stats(ctx, stats).await
    }
}
