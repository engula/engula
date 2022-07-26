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
    collections::{HashMap, HashSet, LinkedList},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use engula_api::server::v1::{group_request_union::Request, *};
use tracing::{debug, error, info, trace, warn};

use crate::{
    node::{
        replica::{ExecCtx, ReplicaConfig},
        Replica,
    },
    raftgroup::{RaftGroupState, RaftNodeFacade},
    root::RemoteStore,
    runtime::{sync::WaitGroup, TaskPriority},
    serverpb::v1::*,
    Provider,
};

/// The task scheduler of an replica.
pub struct Scheduler {
    #[allow(unused)]
    cfg: ReplicaConfig,
    ctx: ScheduleContext,

    replica_states: Arc<Mutex<Vec<ReplicaState>>>,

    // These replicas are involved in scheduling.
    locked_replicas: HashSet<u64>,

    // A group only can have one change config task.
    change_config_task: Option<ChangeConfigTask>,
    tasks: LinkedList<ScheduleTask>,
}

struct ScheduleContext {
    replica_id: u64,
    group_id: u64,
    required_replicas: usize,

    replica: Arc<Replica>,
    raft_node: RaftNodeFacade,
    provider: Arc<Provider>,
    root_store: RemoteStore,

    current_term: u64,

    lost_peers: HashMap<u64, Instant>,
    orphan_replicas: HashMap<u64, Instant>,
    replica_states: Vec<ReplicaState>,

    /// The number of voters of current group, includes both `Voter` and `IncomingVoter`.
    num_voters: usize,
}

impl Scheduler {
    async fn run(&mut self, current_term: u64) {
        self.ctx.current_term = current_term;
        self.recover_pending_tasks().await;
        while let Ok(Some(term)) = self.ctx.replica.on_leader("scheduler", true).await {
            if term != current_term {
                break;
            }
            crate::runtime::time::sleep(Duration::from_secs(1)).await;

            if let Some(desc) = self.check_group_state().await {
                self.advance_tasks(&desc).await;
            };
        }
    }

    async fn check_group_state(&mut self) -> Option<GroupDesc> {
        if let Some(state) = self.ctx.raft_node.raft_group_state().await {
            let replica_states = { self.replica_states.lock().unwrap().clone() };
            let desc = self.ctx.replica.descriptor();
            if self.ctx.replica.check_lease().await.is_ok() {
                self.ctx
                    .apply_raft_group_state(&state, &desc, replica_states);
                self.check_config_change_state().await;
                self.check_replica_states(&desc).await;
                return Some(desc);
            }
        }
        None
    }

    async fn check_replica_states(&mut self, desc: &GroupDesc) {
        let now = Instant::now();
        for s in &self.ctx.replica_states {
            let instant = match self.ctx.orphan_replicas.get(&s.replica_id) {
                Some(instant) => instant,
                None => continue,
            };
            if *instant + Duration::from_secs(60) > now
                && !self
                    .cfg
                    .testing_knobs
                    .disable_orphan_replica_detecting_intervals
            {
                continue;
            }

            if !self.locked_replicas.insert(s.replica_id) {
                continue;
            }

            let replica_id = s.replica_id;
            let group_id = s.group_id;
            for r in &desc.replicas {
                if r.id == replica_id {
                    panic!(
                        "replica {replica_id} belongs to group {group_id}, it must not a orphan replica",
                    );
                }
            }

            info!("group {group_id} find a orphan replica {replica_id}, try remove it",);

            let replica = ReplicaDesc {
                id: replica_id,
                node_id: s.node_id,
                ..Default::default()
            };
            self.tasks.push_back(ScheduleTask {
                value: Some(schedule_task::Value::RemoveReplica(RemoveReplicaTask {
                    replica: Some(replica),
                    group: Some(desc.clone()),
                })),
            });
        }
    }

    async fn check_config_change_state(&mut self) {
        if self.change_config_task.is_none() {
            if self.ctx.is_group_sicked() {
                self.setup_curing_group_task().await;
            } else if self.ctx.is_group_promotable() {
                self.setup_promoting_group_task().await;
            }
        }
    }

    async fn recover_pending_tasks(&mut self) {
        self.change_config_task = None;
        self.tasks.clear();
        self.locked_replicas.clear();
        self.ctx.lost_peers.clear();
        self.ctx.replica_states.clear();
        self.load_pending_tasks().await;
    }

    async fn load_pending_tasks(&mut self) {
        // TODO(walter) load pending tasks from disk.
    }

    async fn setup_promoting_group_task(&mut self) {
        let acquire_replicas = self.ctx.num_missing_replicas();
        if acquire_replicas == 0 {
            return;
        }

        let replicas = match self
            .ctx
            .alloc_addition_replicas("promoting_group", acquire_replicas)
            .await
        {
            Some(replicas) => replicas,
            None => return,
        };

        let incoming_peers = replicas.iter().map(|r| r.id).collect::<Vec<_>>();

        info!(
            group = self.ctx.group_id,
            replica = self.ctx.replica_id,
            "promote group by add {incoming_peers:?}"
        );
        self.change_config_task = Some(ChangeConfigTask::add_replicas(replicas));
        for id in incoming_peers {
            assert!(self.locked_replicas.insert(id));
        }
    }

    async fn setup_curing_group_task(&mut self) {
        let outgoing_voters = self.ctx.lost_replicas();
        if outgoing_voters.is_empty() {
            return;
        }
        for voter in &outgoing_voters {
            if self.locked_replicas.contains(&voter.id) {
                return;
            }
        }

        let replicas = match self
            .ctx
            .alloc_addition_replicas("curing_group", outgoing_voters.len())
            .await
        {
            Some(replicas) => replicas,
            None => return,
        };

        let incoming_peers = replicas.iter().map(|r| r.id).collect::<Vec<_>>();
        let outgoing_peers = outgoing_voters.iter().map(|r| r.id).collect::<Vec<_>>();

        info!(
            group = self.ctx.group_id,
            replica = self.ctx.replica_id,
            "try cure group by replacing {outgoing_peers:?} with {incoming_peers:?}"
        );

        self.change_config_task = Some(ChangeConfigTask::replace_voters(replicas, outgoing_voters));
        for id in incoming_peers {
            assert!(self.locked_replicas.insert(id));
        }
        for id in outgoing_peers {
            assert!(self.locked_replicas.insert(id));
        }
    }

    async fn advance_tasks(&mut self, desc: &GroupDesc) {
        if let Some(task) = self.change_config_task.as_mut() {
            if self.ctx.advance_change_config_task(task, desc).await {
                for id in task.involved_replicas() {
                    self.locked_replicas.remove(&id);
                }
                self.change_config_task = None;
            }
        }

        let mut cursor = self.tasks.cursor_front_mut();
        while let Some(task) = cursor.current() {
            use schedule_task::Value;
            let done = match &mut task.value {
                Some(Value::RemoveReplica(task)) => {
                    self.ctx.advance_remove_replica_task(task).await
                }
                _ => unreachable!(),
            };
            if done {
                for id in task.involved_replicas() {
                    self.locked_replicas.remove(&id);
                }
                cursor.remove_current();
            } else {
                cursor.move_next();
            }
        }
    }
}

impl ScheduleContext {
    fn new(replica: Arc<Replica>, provider: Arc<Provider>) -> Self {
        let info = replica.replica_info();
        let raft_node = replica.raft_node();
        let root_store = RemoteStore::new(provider.clone());
        ScheduleContext {
            replica_id: info.replica_id,
            group_id: info.group_id,
            replica,
            raft_node,
            provider,
            root_store,
            current_term: 0,
            lost_peers: HashMap::default(),
            orphan_replicas: HashMap::default(),
            replica_states: Vec::default(),
            num_voters: 0,
            // FIXME(walter) configurable replica number.
            required_replicas: 3,
        }
    }

    fn apply_raft_group_state(
        &mut self,
        state: &RaftGroupState,
        desc: &GroupDesc,
        replica_states: Vec<ReplicaState>,
    ) {
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

        self.num_voters = desc
            .replicas
            .iter()
            .filter(|r| {
                r.role == ReplicaRole::Voter as i32 || r.role == ReplicaRole::IncomingVoter as i32
            })
            .count();

        let replica_set = desc.replicas.iter().map(|r| r.id).collect::<HashSet<_>>();
        self.orphan_replicas.retain(|k, _| !replica_set.contains(k));
        self.replica_states = replica_states;
        for r in &self.replica_states {
            if !replica_set.contains(&r.replica_id) {
                self.orphan_replicas
                    .entry(r.replica_id)
                    .or_insert_with(Instant::now);
            }
        }
    }

    fn is_group_sicked(&self) -> bool {
        !self.lost_peers.is_empty()
    }

    fn is_group_promotable(&self) -> bool {
        self.num_voters < self.required_replicas
            && self.required_replicas <= self.provider.router.total_nodes()
    }

    fn num_missing_replicas(&self) -> usize {
        self.required_replicas.saturating_sub(self.num_voters)
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

    async fn advance_remove_replica_task(&mut self, task: &mut RemoveReplicaTask) -> bool {
        if let Some(replica) = task.replica.as_ref() {
            if let Err(e) = self
                .remove_replica(replica, task.group.clone().unwrap_or_default())
                .await
            {
                warn!(
                    group = self.group_id,
                    replica = self.replica_id,
                    "remove replica {replica:?}: {e}"
                );
                return false;
            }

            if let Err(e) = self
                .root_store
                .clear_replica_state(self.group_id, replica.id)
                .await
            {
                warn!(
                    group = self.group_id,
                    replica = self.replica_id,
                    "remove replica state of {replica:?}: {e}"
                );
                return false;
            }

            self.orphan_replicas.remove(&replica.id);
        }

        true
    }

    async fn advance_change_config_task(
        &self,
        task: &mut ChangeConfigTask,
        desc: &GroupDesc,
    ) -> bool {
        if task.next_step(desc).is_some() && self.change_config_next_step(task).await {
            info!(
                group = self.group_id,
                replica = self.replica_id,
                "change config task success"
            );
            return true;
        }
        false
    }

    async fn change_config_next_step(&self, task: &mut ChangeConfigTask) -> bool {
        match TaskStep::from_i32(task.current).unwrap() {
            TaskStep::Initialized => {
                if self
                    .execute_create_replica(task.create_replica.as_mut().unwrap())
                    .await
                {
                    task.current = TaskStep::CreateReplica as i32;
                }
            }
            TaskStep::CreateReplica => {
                if self
                    .execute_add_learner(task.add_learner.as_mut().unwrap())
                    .await
                {
                    task.current = TaskStep::AddLearner as i32;
                }
            }
            TaskStep::AddLearner => {
                if self
                    .execute_replace_voter(task.replace_voter.as_mut().unwrap())
                    .await
                {
                    task.current = TaskStep::ReplaceVoter as i32;
                }
            }
            TaskStep::ReplaceVoter => {
                if self
                    .execute_remove_learner(task.remove_learner.as_mut().unwrap())
                    .await
                {
                    task.current = TaskStep::RemoveLearner as i32;
                }
            }
            TaskStep::RemoveLearner => {
                return true;
            }
        }
        false
    }

    async fn execute_create_replica(&self, step: &mut CreateReplicaStep) -> bool {
        for r in &step.replicas {
            if let Err(e) = self.create_replica(r).await {
                warn!(
                    group = self.group_id,
                    replica = self.replica_id,
                    "create replica {r:?}: {e}"
                );
                return false;
            }
        }

        // step to next step.
        true
    }

    async fn execute_add_learner(&self, step: &AddLearnerStep) -> bool {
        let exec_ctx = ExecCtx::with_epoch(self.replica.epoch());
        let req = Request::ChangeReplicas(step.into());
        if let Err(e) = self.replica.execute(exec_ctx, &req).await {
            warn!(
                group = self.group_id,
                replica = self.replica_id,
                "add learner step: {e}, retry this step later"
            );
            false
        } else {
            info!(
                group = self.group_id,
                replica = self.replica_id,
                "add learner step is executed"
            );
            true
        }
    }

    async fn execute_replace_voter(&self, step: &ReplaceVoterStep) -> bool {
        let exec_ctx = ExecCtx::with_epoch(self.replica.epoch());
        let req = Request::ChangeReplicas(step.into());
        if let Err(e) = self.replica.execute(exec_ctx, &req).await {
            warn!(
                group = self.group_id,
                replica = self.replica_id,
                "replace voters step: {e}, retry this step later"
            );
            false
        } else {
            info!(
                group = self.group_id,
                replica = self.replica_id,
                "replace voters step is executed"
            );
            true
        }
    }

    async fn execute_remove_learner(&self, step: &RemoveLearnerStep) -> bool {
        let exec_ctx = ExecCtx::with_epoch(self.replica.epoch());
        let req = Request::ChangeReplicas(step.into());
        if let Err(e) = self.replica.execute(exec_ctx, &req).await {
            warn!(
                group = self.group_id,
                replica = self.replica_id,
                "remove learner step: {e}, retry this step later"
            );
            false
        } else {
            info!(
                group = self.group_id,
                replica = self.replica_id,
                "remove learner step is executed"
            );
            true
        }
    }

    /// Alloc addition replicas from root.
    async fn alloc_addition_replicas(
        &mut self,
        who: &str,
        num_required: usize,
    ) -> Option<Vec<ReplicaDesc>> {
        let req = AllocReplicaRequest {
            group_id: self.group_id,
            epoch: self.replica.epoch(),
            current_term: self.current_term,
            leader_id: self.replica_id,
            num_required: num_required as u64,
        };
        match self.provider.root_client.alloc_replica(req).await {
            Ok(resp) => Some(resp.replicas),
            Err(
                e @ (engula_client::Error::ResourceExhausted(_)
                | engula_client::Error::EpochNotMatch(_)),
            ) => {
                debug!(
                    replica = self.replica_id,
                    group = self.group_id,
                    "alloc addition replicas for {who}: {e}",
                );
                None
            }
            Err(e) => {
                error!(
                    replica = self.replica_id,
                    group = self.group_id,
                    "alloc addition replicas for {who}: {e}",
                );
                None
            }
        }
    }

    async fn create_replica(
        &self,
        r: &ReplicaDesc,
    ) -> std::result::Result<(), engula_client::Error> {
        let addr = self.provider.router.find_node_addr(r.node_id)?;
        let client = self.provider.conn_manager.get_node_client(addr).await?;
        let desc = GroupDesc {
            id: self.group_id,
            ..Default::default()
        };
        client.create_replica(r.id, desc).await?;
        Ok(())
    }

    async fn remove_replica(
        &self,
        r: &ReplicaDesc,
        group: GroupDesc,
    ) -> std::result::Result<(), engula_client::Error> {
        let addr = self.provider.router.find_node_addr(r.node_id)?;
        let client = self.provider.conn_manager.get_node_client(addr).await?;
        client.remove_replica(r.id, group.clone()).await?;
        Ok(())
    }
}

impl From<&AddLearnerStep> for ChangeReplicasRequest {
    fn from(step: &AddLearnerStep) -> Self {
        let changes = ChangeReplicas {
            changes: step.replicas.iter().map(replica_as_learner).collect(),
        };
        ChangeReplicasRequest {
            change_replicas: Some(changes),
        }
    }
}

impl From<&ReplaceVoterStep> for ChangeReplicasRequest {
    fn from(step: &ReplaceVoterStep) -> Self {
        let mut changes = step
            .incoming_voters
            .iter()
            .map(replica_as_incoming_voter)
            .collect::<Vec<_>>();
        changes.extend(step.outgoing_voters.iter().map(replica_as_outgoing_voter));
        ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas { changes }),
        }
    }
}

impl From<&RemoveLearnerStep> for ChangeReplicasRequest {
    fn from(step: &RemoveLearnerStep) -> Self {
        let changes = ChangeReplicas {
            changes: step
                .learners
                .iter()
                .map(replica_as_outgoing_voter)
                .collect(),
        };
        ChangeReplicasRequest {
            change_replicas: Some(changes),
        }
    }
}

impl ChangeConfigTask {
    fn involved_replicas(&self) -> HashSet<u64> {
        let mut replicas = HashSet::default();
        self.create_replica
            .as_ref()
            .unwrap()
            .replicas
            .iter()
            .for_each(|r| {
                replicas.insert(r.id);
            });
        self.add_learner
            .as_ref()
            .unwrap()
            .replicas
            .iter()
            .for_each(|r| {
                replicas.insert(r.id);
            });
        self.replace_voter
            .as_ref()
            .unwrap()
            .incoming_voters
            .iter()
            .for_each(|r| {
                replicas.insert(r.id);
            });
        self.replace_voter
            .as_ref()
            .unwrap()
            .outgoing_voters
            .iter()
            .for_each(|r| {
                replicas.insert(r.id);
            });
        replicas
    }

    fn add_replicas(incoming_voters: Vec<ReplicaDesc>) -> Self {
        let create_replica_step = CreateReplicaStep {
            replicas: incoming_voters.clone(),
        };
        let add_learner_step = AddLearnerStep {
            replicas: incoming_voters.clone(),
        };
        let replace_voter_step = ReplaceVoterStep {
            incoming_voters,
            outgoing_voters: vec![],
        };

        ChangeConfigTask {
            current: TaskStep::Initialized as i32,
            create_replica: Some(create_replica_step),
            add_learner: Some(add_learner_step),
            replace_voter: Some(replace_voter_step),
            remove_learner: Some(RemoveLearnerStep { learners: vec![] }),
        }
    }

    fn replace_voters(
        incoming_voters: Vec<ReplicaDesc>,
        outgoing_voters: Vec<ReplicaDesc>,
    ) -> Self {
        let create_replica_step = CreateReplicaStep {
            replicas: incoming_voters.clone(),
        };
        let add_learner_step = AddLearnerStep {
            replicas: incoming_voters.clone(),
        };
        let remove_learner_step = RemoveLearnerStep {
            learners: outgoing_voters.clone(),
        };
        let replace_voter_step = ReplaceVoterStep {
            incoming_voters,
            outgoing_voters,
        };

        ChangeConfigTask {
            current: TaskStep::Initialized as i32,
            create_replica: Some(create_replica_step),
            add_learner: Some(add_learner_step),
            replace_voter: Some(replace_voter_step),
            remove_learner: Some(remove_learner_step),
        }
    }

    /// Check whether can go to the next step?
    fn next_step(&self, desc: &GroupDesc) -> Option<()> {
        match TaskStep::from_i32(self.current).unwrap() {
            TaskStep::Initialized | TaskStep::CreateReplica => {
                return Some(());
            }
            TaskStep::AddLearner => {
                if self
                    .add_learner
                    .as_ref()
                    .expect("For TaskStep::AddLearner, add_learner is not None")
                    .is_finished(desc)
                {
                    // TODO(walter) check applied index.
                    return Some(());
                }
            }
            TaskStep::ReplaceVoter => {
                if self
                    .replace_voter
                    .as_ref()
                    .expect("For TaskStep::ReplaceVoter, replace_voter is not None")
                    .is_finished(desc)
                {
                    return Some(());
                }
            }
            TaskStep::RemoveLearner => {
                if self
                    .remove_learner
                    .as_ref()
                    .expect("For TaskStep::RemoveLearner, remove_learner is not None")
                    .is_finished(desc)
                {
                    return Some(());
                }
            }
        }
        None
    }
}

impl AddLearnerStep {
    fn is_finished(&self, desc: &GroupDesc) -> bool {
        let mut learners = self.replicas.iter().map(|r| r.id).collect::<HashSet<_>>();
        for replica in &desc.replicas {
            if replica.role == ReplicaRole::Learner as i32 {
                learners.remove(&replica.id);
            }
        }
        learners.is_empty()
    }
}

impl ReplaceVoterStep {
    fn is_finished(&self, desc: &GroupDesc) -> bool {
        let mut incoming_voters = self
            .incoming_voters
            .iter()
            .map(|r| r.id)
            .collect::<HashSet<_>>();
        for replica in &desc.replicas {
            if replica.role == ReplicaRole::Voter as i32 {
                incoming_voters.remove(&replica.id);
            }
        }
        incoming_voters.is_empty()
    }
}

impl RemoveLearnerStep {
    fn is_finished(&self, desc: &GroupDesc) -> bool {
        let learners = self.learners.iter().map(|r| r.id).collect::<HashSet<_>>();
        for replica in &desc.replicas {
            if learners.contains(&replica.id) {
                return false;
            }
        }
        true
    }
}

impl ScheduleTask {
    fn involved_replicas(&self) -> HashSet<u64> {
        use schedule_task::Value;

        let mut replicas = HashSet::default();
        #[allow(clippy::single_match)]
        match self.value.as_ref() {
            Some(Value::RemoveReplica(RemoveReplicaTask {
                replica: Some(replica),
                ..
            })) => {
                replicas.insert(replica.id);
            }
            _ => {}
        }
        replicas
    }
}

pub(crate) fn setup(
    cfg: ReplicaConfig,
    provider: Arc<Provider>,
    replica: Arc<Replica>,
    wait_group: WaitGroup,
) {
    let group_id = replica.replica_info().group_id;
    let tag = &group_id.to_le_bytes();
    let executor = provider.executor.clone();

    let cloned_replica = replica.clone();
    let cloned_wait_group = wait_group.clone();
    let root_store = RemoteStore::new(provider.clone());
    let replica_states = Arc::new(Mutex::new(Vec::default()));
    let cloned_replica_states = replica_states.clone();
    let cloned_cfg = cfg.clone();
    executor.spawn(Some(tag), TaskPriority::Low, async move {
        watch_replica_states(
            cloned_cfg,
            root_store,
            cloned_replica,
            cloned_replica_states,
        )
        .await;
        drop(cloned_wait_group);
    });
    executor.spawn(Some(tag), TaskPriority::Low, async move {
        scheduler_main(cfg, provider, replica, replica_states).await;
        drop(wait_group);
    });
}

async fn watch_replica_states(
    cfg: ReplicaConfig,
    root_store: RemoteStore,
    replica: Arc<Replica>,
    replica_states: Arc<Mutex<Vec<ReplicaState>>>,
) {
    let info = replica.replica_info();
    let group_id = info.group_id;
    while let Ok(Some(_)) = replica.on_leader("replica-state-watcher", false).await {
        match root_store.list_replica_state(group_id).await {
            Ok(states) => {
                trace!("list replica states of group {group_id}: {:?}", states);
                *replica_states.lock().unwrap() = states;
            }
            Err(e) => {
                error!("watch replica states of group {group_id}: {e:?}");
            }
        }

        if !cfg.testing_knobs.disable_orphan_replica_detecting_intervals {
            crate::runtime::time::sleep(Duration::from_secs(31)).await;
        }
    }
    debug!("replica state watcher of group {group_id} is stopped");
}

async fn scheduler_main(
    cfg: ReplicaConfig,
    provider: Arc<Provider>,
    replica: Arc<Replica>,
    replica_states: Arc<Mutex<Vec<ReplicaState>>>,
) {
    let group_id = replica.replica_info().group_id;
    let mut scheduler = Scheduler {
        cfg,
        ctx: ScheduleContext::new(replica, provider),
        replica_states,
        locked_replicas: HashSet::default(),
        change_config_task: None,
        tasks: LinkedList::default(),
    };
    while let Ok(Some(current_term)) = scheduler.ctx.replica.on_leader("scheduler", false).await {
        scheduler.run(current_term).await;
    }
    debug!("scheduler of group {group_id} is stopped");
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
