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
    time::Instant,
};

use engula_api::server::v1::{group_request_union::Request, *};
use engula_client::Router;
use tracing::{error, info};

use crate::{
    node::{replica::ExecCtx, Replica},
    raftgroup::{RaftGroupState, RaftNodeFacade},
    serverpb::v1::*,
    Result,
};

/// The task scheduler of an replica.
pub struct Scheduler {
    ctx: ScheduleContext,

    cure_group_task: Option<CureGroupTask>,
}

struct ScheduleContext {
    replica_id: u64,
    group_id: u64,
    replica: Arc<Replica>,
    raft_node: RaftNodeFacade,
    router: Router,

    lost_peers: HashMap<u64, Instant>,
}

impl Scheduler {
    async fn run(&mut self, current_term: u64) {
        self.recover_pending_tasks().await;
        while let Ok(Some(term)) = self.ctx.replica.on_leader(true).await {
            if term != current_term {
                break;
            }
            self.check_replica_state().await;

            let desc = self.ctx.replica.descriptor();
            self.advance_tasks(&desc).await;
        }
    }

    async fn check_replica_state(&mut self) {
        if let Some(state) = self.ctx.raft_node.raft_group_state().await {
            self.ctx.apply_raft_group_state(&state);
            if self.cure_group_task.is_none() && self.ctx.is_group_sicked() {
                self.setup_cure_group_task().await;
            }
        }
    }

    async fn recover_pending_tasks(&mut self) {
        self.load_pending_tasks().await;
    }

    async fn load_pending_tasks(&mut self) {
        // TODO(walter) load pending tasks from disk.
    }

    async fn setup_cure_group_task(&mut self) {
        let replicas = match self.alloc_addition_replicas().await {
            Ok(replicas) => replicas,
            Err(e) => {
                error!(
                    replica = self.ctx.replica_id,
                    group = self.ctx.group_id,
                    "alloc addition replicas: {}",
                    e
                );
                return;
            }
        };

        let outgoing_voters = self.ctx.lost_replicas();
        let create_replica_step = CreateReplicaStep {
            replicas: replicas.clone(),
        };
        let add_learner_step = AddLearnerStep {
            replicas: replicas.clone(),
        };
        let replace_voter_step = ReplaceVoterStep {
            incoming_voters: replicas,
            outgoing_voters,
        };

        let task = CureGroupTask {
            current: TaskStep::Initialized as i32,
            create_replica: Some(create_replica_step),
            add_learner: Some(add_learner_step),
            replace_voter: Some(replace_voter_step),
        };
        self.cure_group_task = Some(task);
    }

    async fn advance_tasks(&mut self, desc: &GroupDesc) {
        if let Some(task) = self.cure_group_task.as_mut() {
            let task_finished = match self.ctx.advance_cure_group_task(task, desc).await {
                Some(true) => self.ctx.execute_cure_group(task).await,
                Some(false) => true,
                None => false,
            };
            if task_finished {
                info!(
                    replica = self.ctx.replica_id,
                    group = self.ctx.group_id,
                    "replace lost replicas success"
                );
                self.cure_group_task = None;
            }
        }
    }

    async fn alloc_addition_replicas(&mut self) -> Result<Vec<ReplicaDesc>> {
        todo!("alloc id from root")
    }
}

impl ScheduleContext {
    fn new(replica: Arc<Replica>, router: Router) -> Self {
        let info = replica.replica_info();
        let raft_node = replica.raft_node();
        ScheduleContext {
            replica_id: info.replica_id,
            group_id: info.group_id,
            replica,
            raft_node,
            router,
            lost_peers: HashMap::default(),
        }
    }

    fn apply_raft_group_state(&mut self, state: &RaftGroupState) {
        let lost_peers = state
            .peers
            .iter()
            .filter(|(_, s)| s.might_lost)
            .map(|(id, _)| *id)
            .collect::<HashSet<_>>();
        self.lost_peers.retain(|k, _| lost_peers.contains(k));
        for id in lost_peers {
            self.lost_peers.entry(id).or_insert_with(Instant::now);
        }
    }

    fn is_group_sicked(&self) -> bool {
        !self.lost_peers.is_empty()
    }

    fn lost_replicas(&self) -> Vec<ReplicaDesc> {
        let desc = self.replica.descriptor();
        let mut replicas = vec![];
        for id in self.lost_peers.keys() {
            if let Some(replica) = desc.replicas.iter().find(|v| v.id == *id) {
                replicas.push(replica.clone());
            }
        }
        replicas
    }

    #[allow(dead_code)]
    async fn dispatch(&self, task: &mut ScheduleTask) -> bool {
        use schedule_task::Value;
        match task
            .value
            .as_mut()
            .expect("ScheduleTask::value is not None")
        {
            Value::CureGroup(task) => self.execute_cure_group(task).await,
        }
    }

    async fn advance_cure_group_task(
        &self,
        task: &mut CureGroupTask,
        desc: &GroupDesc,
    ) -> Option<bool> {
        match TaskStep::from_i32(task.current).unwrap() {
            TaskStep::Initialized => {
                task.current = TaskStep::CreateReplica as i32;
                return Some(true);
            }
            TaskStep::CreateReplica => unreachable!(),
            TaskStep::AddLearner => {
                let mut target_learners = task
                    .add_learner
                    .as_ref()
                    .unwrap()
                    .replicas
                    .iter()
                    .map(|r| r.id)
                    .collect::<HashSet<_>>();
                for replica in &desc.replicas {
                    if replica.role == ReplicaRole::Learner as i32 {
                        target_learners.remove(&replica.id);
                    }
                }
                if target_learners.is_empty() {
                    // TODO(walter) check applied index.
                    task.current = TaskStep::ReplaceVoter as i32;
                    return Some(true);
                }
            }
            TaskStep::ReplaceVoter => {
                let mut incoming_voters = task
                    .replace_voter
                    .as_ref()
                    .unwrap()
                    .incoming_voters
                    .iter()
                    .map(|r| r.id)
                    .collect::<HashSet<_>>();
                for replica in &desc.replicas {
                    if replica.role == ReplicaRole::Voter as i32 {
                        incoming_voters.remove(&replica.id);
                    }
                }
                if incoming_voters.is_empty() {
                    // Task is finished.
                    return Some(false);
                }
            }
        }
        None
    }

    async fn execute_cure_group(&self, task: &mut CureGroupTask) -> bool {
        loop {
            match TaskStep::from_i32(task.current).unwrap() {
                TaskStep::Initialized => {
                    task.current = TaskStep::CreateReplica as i32;
                }
                TaskStep::CreateReplica => {
                    if self
                        .execute_create_replica(task.create_replica.as_mut().unwrap())
                        .await
                    {
                        task.current = TaskStep::AddLearner as i32;
                        continue;
                    }
                }
                TaskStep::AddLearner => {
                    self.execute_add_learner(task.add_learner.as_mut().unwrap())
                        .await;
                }
                TaskStep::ReplaceVoter => {
                    self.execute_replace_voter(task.replace_voter.as_mut().unwrap())
                        .await;
                    return true;
                }
            };
            return false;
        }
    }

    async fn execute_create_replica(&self, step: &mut CreateReplicaStep) -> bool {
        use engula_client::NodeClient;

        for r in &step.replicas {
            if let Ok(addr) = self.router.find_node_addr(r.node_id) {
                let client = NodeClient::connect(addr).await.unwrap();
                let desc = GroupDesc {
                    id: self.group_id,
                    ..Default::default()
                };
                client.create_replica(r.id, desc).await.unwrap();
                continue;
            }
            todo!("retry later");
        }

        // step to next step.
        true
    }

    async fn execute_add_learner(&self, step: &AddLearnerStep) {
        let exec_ctx = ExecCtx {
            epoch: self.replica.epoch(),
            ..Default::default()
        };
        let changes = ChangeReplicas {
            changes: step.replicas.iter().map(replica_as_learner).collect(),
        };
        let req = ChangeReplicasRequest {
            change_replicas: Some(changes),
        };
        self.replica
            .execute(exec_ctx, &Request::ChangeReplicas(req))
            .await
            .unwrap();
    }

    async fn execute_replace_voter(&self, step: &ReplaceVoterStep) {
        let exec_ctx = ExecCtx {
            epoch: self.replica.epoch(),
            ..Default::default()
        };
        let mut changes = step
            .incoming_voters
            .iter()
            .map(replica_as_incoming_voter)
            .collect::<Vec<_>>();
        changes.extend(step.outgoing_voters.iter().map(replica_as_outgoing_voter));
        let req = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas { changes }),
        };
        self.replica
            .execute(exec_ctx, &Request::ChangeReplicas(req))
            .await
            .unwrap();
    }
}

pub async fn scheduler_main(router: Router, replica: Arc<Replica>) {
    let mut scheduler = Scheduler {
        ctx: ScheduleContext::new(replica, router),
        cure_group_task: None,
    };
    while let Ok(Some(current_term)) = scheduler.ctx.replica.on_leader(false).await {
        scheduler.run(current_term).await;
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
