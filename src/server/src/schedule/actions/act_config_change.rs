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
use std::{collections::HashSet, sync::Arc};

use engula_api::server::v1::{group_request_union::Request, *};
use tracing::{info, warn};

use super::{Action, ActionState};
use crate::{
    node::replica::ExecCtx,
    schedule::{event_source::EventSource, provider::GroupProviders, scheduler::ScheduleContext},
};

pub struct AddLearners {
    pub providers: Arc<GroupProviders>,
    pub learners: Vec<ReplicaDesc>,
}

pub struct RemoveLearners {
    pub providers: Arc<GroupProviders>,
    pub learners: Vec<ReplicaDesc>,
}

pub struct ReplaceVoters {
    pub providers: Arc<GroupProviders>,

    pub incoming_voters: Vec<ReplicaDesc>,
    pub demoting_voters: Vec<ReplicaDesc>,
}

#[crate::async_trait]
impl Action for AddLearners {
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;

        let changes = ChangeReplicas {
            changes: self.learners.iter().map(replica_as_learner).collect(),
        };
        let cc = ChangeReplicasRequest {
            change_replicas: Some(changes),
        };
        let exec_ctx = ExecCtx::with_epoch(ctx.replica.epoch());
        let req = Request::ChangeReplicas(cc);
        if let Err(e) = ctx.replica.execute(exec_ctx, &req).await {
            warn!("group {group_id} replica {replica_id} task {task_id} add learners: {e}");
            // TODO(walter) add fire condition.
            ActionState::Pending(None)
        } else {
            info!("group {group_id} replica {replica_id} task {task_id} add learners success");
            self.providers.descriptor.watch(task_id);
            ActionState::Done
        }
    }

    async fn poll(&mut self, task_id: u64, _ctx: &mut ScheduleContext<'_>) -> ActionState {
        let replicas = self.providers.descriptor.replicas();
        let mut learners = self.learners.iter().map(|r| r.id).collect::<HashSet<_>>();
        for replica in &replicas {
            if replica.role == ReplicaRole::Learner as i32 {
                learners.remove(&replica.id);
            }
        }
        if learners.is_empty() {
            ActionState::Done
        } else {
            self.providers.descriptor.watch(task_id);
            ActionState::Pending(None)
        }
    }
}

#[crate::async_trait]
impl Action for RemoveLearners {
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;

        let changes = ChangeReplicas {
            changes: self
                .learners
                .iter()
                .map(replica_as_outgoing_voter)
                .collect(),
        };
        let cc = ChangeReplicasRequest {
            change_replicas: Some(changes),
        };
        let exec_ctx = ExecCtx::with_epoch(ctx.replica.epoch());
        let req = Request::ChangeReplicas(cc);
        if let Err(e) = ctx.replica.execute(exec_ctx, &req).await {
            warn!("group {group_id} replica {replica_id} task {task_id} remove learners: {e}");
            ActionState::Pending(None)
        } else {
            info!("group {group_id} replica {replica_id} task {task_id} remove learners success");
            self.providers.descriptor.watch(task_id);
            ActionState::Done
        }
    }

    async fn poll(&mut self, task_id: u64, _ctx: &mut ScheduleContext<'_>) -> ActionState {
        let replicas = self.providers.descriptor.replicas();

        let learners = self.learners.iter().map(|r| r.id).collect::<HashSet<_>>();
        for replica in &replicas {
            if learners.contains(&replica.id) {
                self.providers.descriptor.watch(task_id);
                return ActionState::Pending(None);
            }
        }
        ActionState::Done
    }
}

#[crate::async_trait]
impl Action for ReplaceVoters {
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;

        let mut changes = self
            .incoming_voters
            .iter()
            .map(replica_as_incoming_voter)
            .collect::<Vec<_>>();
        changes.extend(self.demoting_voters.iter().map(replica_as_outgoing_voter));
        let cc = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas { changes }),
        };
        let exec_ctx = ExecCtx::with_epoch(ctx.replica.epoch());
        let req = Request::ChangeReplicas(cc);
        if let Err(e) = ctx.replica.execute(exec_ctx, &req).await {
            warn!("group {group_id} replica {replica_id} task {task_id} replace voters: {e}");
            ActionState::Pending(None)
        } else {
            info!("group {group_id} replica {replica_id} task {task_id} replace voters success");
            self.providers.descriptor.watch(task_id);
            ActionState::Done
        }
    }

    async fn poll(&mut self, task_id: u64, _ctx: &mut ScheduleContext<'_>) -> ActionState {
        let replicas = self.providers.descriptor.replicas();

        let mut incoming_voters = self
            .incoming_voters
            .iter()
            .map(|r| r.id)
            .collect::<HashSet<_>>();
        for replica in &replicas {
            if replica.role == ReplicaRole::Voter as i32 {
                incoming_voters.remove(&replica.id);
            }
        }
        if incoming_voters.is_empty() {
            ActionState::Done
        } else {
            self.providers.descriptor.watch(task_id);
            ActionState::Pending(None)
        }
    }
}

fn replica_as_learner(r: &ReplicaDesc) -> ChangeReplica {
    ChangeReplica {
        replica_id: r.id,
        node_id: r.node_id,
        change_type: ChangeReplicaType::AddLearner as i32,
    }
}

fn replica_as_incoming_voter(r: &ReplicaDesc) -> ChangeReplica {
    ChangeReplica {
        replica_id: r.id,
        node_id: r.node_id,
        change_type: ChangeReplicaType::Add as i32,
    }
}

fn replica_as_outgoing_voter(r: &ReplicaDesc) -> ChangeReplica {
    ChangeReplica {
        replica_id: r.id,
        node_id: r.node_id,
        change_type: ChangeReplicaType::Remove as i32,
    }
}
