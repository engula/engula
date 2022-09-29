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
use std::{collections::HashSet, sync::Arc, time::Duration};

use engula_api::server::v1::{group_request_union::Request, *};
use tracing::{info, warn};

use super::{Action, ActionState};
use crate::{
    error::BusyReason,
    node::{replica::ExecCtx, Replica},
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
        let changes = ChangeReplicas {
            changes: self.learners.iter().map(replica_as_learner).collect(),
        };
        let cc = ChangeReplicasRequest {
            change_replicas: Some(changes),
        };
        let req = Request::ChangeReplicas(cc);
        let action_state =
            try_execute(ctx.replica.as_ref(), task_id, &req, "adding learners").await;
        if matches!(&action_state, ActionState::Done) {
            self.providers.descriptor.watch(task_id);
        }
        action_state
    }

    async fn poll(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let replicas = self.providers.descriptor.replicas();
        let mut learners = self.learners.iter().map(|r| r.id).collect::<HashSet<_>>();
        for replica in &replicas {
            if replica.role == ReplicaRole::Learner as i32 {
                learners.remove(&replica.id);
            }
        }
        if learners.is_empty() {
            let group_id = ctx.group_id;
            let replica_id = ctx.replica_id;
            info!("group {group_id} replica {replica_id} task {task_id} adding learners step done");

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
        let req = Request::ChangeReplicas(cc);
        let action_state =
            try_execute(ctx.replica.as_ref(), task_id, &req, "removing learners").await;
        if matches!(&action_state, ActionState::Done) {
            self.providers.descriptor.watch(task_id);
        }
        action_state
    }

    async fn poll(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let replicas = self.providers.descriptor.replicas();

        let learners = self.learners.iter().map(|r| r.id).collect::<HashSet<_>>();
        for replica in &replicas {
            if learners.contains(&replica.id) {
                self.providers.descriptor.watch(task_id);
                return ActionState::Pending(None);
            }
        }

        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;
        info!("group {group_id} replica {replica_id} task {task_id} removing learners step done");

        ActionState::Done
    }
}

#[crate::async_trait]
impl Action for ReplaceVoters {
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let mut changes = self
            .incoming_voters
            .iter()
            .map(replica_as_incoming_voter)
            .collect::<Vec<_>>();
        changes.extend(self.demoting_voters.iter().map(replica_as_outgoing_voter));
        let cc = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas { changes }),
        };
        let req = Request::ChangeReplicas(cc);
        let action_state =
            try_execute(ctx.replica.as_ref(), task_id, &req, "replacing voters").await;
        if matches!(&action_state, ActionState::Done) {
            self.providers.descriptor.watch(task_id);
        }
        action_state
    }

    async fn poll(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
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
            let group_id = ctx.group_id;
            let replica_id = ctx.replica_id;
            info!(
                "group {group_id} replica {replica_id} task {task_id} replacing voters step done"
            );

            ActionState::Done
        } else {
            self.providers.descriptor.watch(task_id);
            ActionState::Pending(None)
        }
    }
}

fn try_execute<'a>(
    replica: &'a Replica,
    task_id: u64,
    request: &'a Request,
    desc: &'static str,
) -> impl std::future::Future<Output = ActionState> + 'a {
    async move {
        use crate::Error;

        let info = replica.replica_info();
        let group_id = info.group_id;
        let replica_id = info.replica_id;

        let exec_ctx = ExecCtx::with_epoch(replica.epoch());
        match replica.try_execute(exec_ctx, request).await {
            Err(Error::ServiceIsBusy(BusyReason::AclGuard | BusyReason::PendingConfigChange)) => {
                ActionState::Pending(Some(Duration::from_millis(100)))
            }
            Err(e) => {
                warn!("group {group_id} replica {replica_id} task {task_id} abort {desc}: {e}");
                ActionState::Aborted
            }
            Ok(_) => {
                info!(
                    "group {group_id} replica {replica_id} task {task_id} execute {desc} success"
                );
                ActionState::Done
            }
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
