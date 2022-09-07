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

use std::{collections::LinkedList, sync::Arc};

use engula_api::server::v1::*;
use engula_client::{GroupClient, RouterGroupState};
use prometheus::HistogramTimer;
use tokio::{sync::Mutex, time::Instant};
use tracing::{error, info, warn};

use super::{allocator::*, metrics, *};
use crate::{
    bootstrap::ROOT_GROUP_ID,
    serverpb::v1::{reconcile_task::Task, *},
    Result,
};

pub struct ReconcileScheduler {
    ctx: ScheduleContext,
    tasks: Mutex<LinkedList<ReconcileTask>>,
}

pub struct ScheduleContext {
    shared: Arc<RootShared>,
    alloc: Arc<Allocator<SysAllocSource>>,
    heartbeat_queue: Arc<HeartbeatQueue>,
    ongoing_stats: Arc<OngoingStats>,
    jobs: Arc<Jobs>,
    cfg: RootConfig,
}

impl ReconcileScheduler {
    pub fn new(ctx: ScheduleContext) -> Self {
        Self {
            ctx,
            tasks: Default::default(),
        }
    }

    pub async fn step_one(&self) -> Duration {
        let cr = self.check(1).await; // TODO: take care self.tasks then can give more > 1 value here.
        if cr.is_ok() && cr.unwrap() {
            let _step_timer = metrics::RECONCILE_STEP_DURATION_SECONDS.start_timer();
            let immediately_next = self.advance_tasks().await;
            if immediately_next {
                self.ctx.heartbeat_queue.wait_one_heartbeat_tick().await;
                return Duration::ZERO;
            }
        }
        _ = Duration::from_secs(self.ctx.cfg.schedule_interval_sec);
        Duration::from_secs(4)
    }

    pub async fn wait_heartbeat_tick(&self) {
        self.ctx.heartbeat_queue.wait_one_heartbeat_tick().await
    }

    pub async fn setup_task(&self, task: ReconcileTask) {
        let mut tasks = self.tasks.lock().await;
        tasks.push_back(task.to_owned());
        info!(len = tasks.len(), task=?task, "setup new reconcile task")
    }

    async fn is_empty(&self) -> bool {
        self.tasks.lock().await.is_empty()
    }
}

impl ReconcileScheduler {
    pub async fn need_reconcile(&self) -> Result<bool> {
        let group_action = self.ctx.alloc.compute_group_action().await?;
        if matches!(group_action, GroupAction::Add(_)) {
            return Ok(true);
        }

        let actions = self.comput_replica_role_action().await?;
        if !actions.is_empty() {
            return Ok(true);
        }

        let shard_actions = self.ctx.alloc.compute_shard_action().await?;
        if !shard_actions.is_empty() {
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn check(&self, _max_try_per_tick: u64) -> Result<bool> {
        let _timer = super::metrics::RECONCILE_CHECK_DURATION_SECONDS.start_timer();
        let group_action = self.ctx.alloc.compute_group_action().await?;
        if let GroupAction::Add(cnt) = group_action {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .cluster_groups
                .set(0);
            for _ in 0..cnt {
                self.ctx
                    .jobs
                    .submit(
                        BackgroundJob {
                            job: Some(Job::CreateOneGroup(CreateOneGroupJob {
                                request_replica_cnt: self.ctx.alloc.replicas_per_group() as u64,
                                status: CreateOneGroupStatus::CreateOneGroupInit as i32,
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        true,
                    )
                    .await?;
            }
            return Ok(true);
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .cluster_groups
            .set(1);

        let ractions = self.comput_replica_role_action().await?;
        let sactions = self.ctx.alloc.compute_shard_action().await?;

        for action in ractions {
            match action {
                ReplicaRoleAction::Replica(ReplicaAction::Migrate(action)) => {
                    self.setup_task(ReconcileTask {
                        task: Some(reconcile_task::Task::ReallocateReplica(
                            ReallocateReplicaTask {
                                group: action.group,
                                epoch: action.epoch,
                                src_node: action.source_node,
                                dest_node: action.dest_node,
                            },
                        )),
                    })
                    .await;
                }
                ReplicaRoleAction::Leader(LeaderAction::Shed(action)) => {
                    self.setup_task(ReconcileTask {
                        task: Some(reconcile_task::Task::TransferGroupLeader(
                            TransferGroupLeaderTask {
                                group: action.group,
                                epoch: action.epoch,
                                src_node: action.src_node,
                                dest_node: action.target_node,
                            },
                        )),
                    })
                    .await;
                }
            }
        }

        for action in sactions {
            let ShardAction::Migrate(action) = action;
            self.setup_task(ReconcileTask {
                task: Some(reconcile_task::Task::MigrateShard(MigrateShardTask {
                    shard: action.shard,
                    src_group: action.source_group,
                    dest_group: action.target_group,
                })),
            })
            .await;
        }

        Ok(!self.is_empty().await)
    }

    pub async fn comput_replica_role_action(&self) -> Result<Vec<ReplicaRoleAction>> {
        let mut actions = Vec::new();

        let mv_repl_action = {
            let repl_cnt_policy = ReplicaCountPolicy::default();
            self.ctx
                .alloc
                .compute_balance_action(&repl_cnt_policy)
                .await?
        };
        if mv_repl_action.is_empty() {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .node_replica_count
                .set(1);
        } else {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .node_replica_count
                .set(0);
        }
        actions.extend_from_slice(
            &mv_repl_action
                .iter()
                .map(|a| {
                    ReplicaRoleAction::Replica(ReplicaAction::Migrate(ReallocateReplica {
                        group: a.group_id,
                        epoch: a.epoch,
                        source_node: a.source_node,
                        dest_node: a.dest_node,
                    }))
                })
                .collect::<Vec<_>>(),
        );

        let leader_actions = {
            let leader_policy = LeaderCountPolicy::default();
            self.ctx
                .alloc
                .compute_balance_action(&leader_policy)
                .await?
        };
        if leader_actions.is_empty() {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .node_leader_count
                .set(1);
        } else {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .node_leader_count
                .set(0);
        }
        actions.extend_from_slice(
            &leader_actions
                .iter()
                .map(|act| {
                    ReplicaRoleAction::Leader(LeaderAction::Shed(TransferLeader {
                        group: act.group_id,
                        src_node: act.source_node,
                        target_node: act.dest_node,
                        epoch: act.epoch,
                    }))
                })
                .collect::<Vec<_>>(),
        );
        Ok(actions)
    }
}

impl ReconcileScheduler {
    async fn advance_tasks(&self) -> bool {
        let mut task = self.tasks.lock().await;
        let mut nowait_next = !task.is_empty();
        metrics::RECONCILE_SCHEDULER_TASK_QUEUE_SIZE.set(task.len() as i64);
        let mut cursor = task.cursor_front_mut();
        while let Some(task) = cursor.current() {
            let _timer = Self::record_exec(task);
            let rs = self.ctx.handle_task(task).await;
            match rs {
                Ok((true /* ack */, immediately_next)) => {
                    cursor.remove_current();
                    if !immediately_next {
                        nowait_next = false
                    }
                }
                _ => {
                    Self::record_retry(task);
                    // ack == false or meet error, skip current task and retry later.
                    cursor.move_next();
                }
            }
        }
        nowait_next
    }

    fn record_exec(task: &mut ReconcileTask) -> HistogramTimer {
        match task.task.as_ref().unwrap() {
            Task::ReallocateReplica(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL
                    .reallocate_replica
                    .inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS
                    .reallocate_replica
                    .start_timer()
            }
            Task::MigrateShard(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.migrate_shard.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS
                    .migrate_shard
                    .start_timer()
            }
            Task::TransferGroupLeader(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.transfer_leader.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS
                    .transfer_leader
                    .start_timer()
            }
            Task::ShedLeader(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL
                    .shed_group_leaders
                    .inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS
                    .shed_group_leaders
                    .start_timer()
            }
            Task::ShedRoot(_) => {
                metrics::RECONCILE_HANDLE_TASK_TOTAL.shed_root_leader.inc();
                metrics::RECONCILE_HANDLE_TASK_DURATION_SECONDS
                    .shed_root_leader
                    .start_timer()
            }
        }
    }

    fn record_retry(task: &mut ReconcileTask) {
        match task.task.as_ref().unwrap() {
            Task::ReallocateReplica(_) => {
                metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc()
            }
            Task::MigrateShard(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.migrate_shard.inc(),
            Task::TransferGroupLeader(_) => {
                metrics::RECONCILE_RETRY_TASK_TOTAL.transfer_leader.inc()
            }
            Task::ShedLeader(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.shed_group_leaders.inc(),
            Task::ShedRoot(_) => metrics::RECONCILE_RETRY_TASK_TOTAL.shed_root_leader.inc(),
        }
    }
}

impl ScheduleContext {
    pub(crate) fn new(
        shared: Arc<RootShared>,
        alloc: Arc<Allocator<SysAllocSource>>,
        heartbeat_queue: Arc<HeartbeatQueue>,
        ongoing_stats: Arc<OngoingStats>,
        jobs: Arc<Jobs>,
        cfg: RootConfig,
    ) -> Self {
        Self {
            shared,
            alloc,
            heartbeat_queue,
            ongoing_stats,
            jobs,
            cfg,
        }
    }

    pub async fn handle_task(
        &self,
        task: &mut ReconcileTask,
    ) -> Result<(
        bool, /* ack current */
        bool, /* immediately step next tick */
    )> {
        info!(task=?task, "handle reconcile task");
        match task.task.as_mut().unwrap() {
            Task::ReallocateReplica(reallocate_replica) => {
                self.handle_reallocate_replica(reallocate_replica).await
            }
            Task::MigrateShard(migrate_shard) => self.handle_migrate_shard(migrate_shard).await,
            Task::TransferGroupLeader(transfer_leader) => {
                self.handle_transfer_leader(transfer_leader).await
            }
            Task::ShedLeader(shed_leader) => self.handle_shed_leader(shed_leader).await,
            Task::ShedRoot(shed_root) => self.handle_shed_root(shed_root).await,
        }
    }

    async fn handle_reallocate_replica(
        &self,
        task: &mut ReallocateReplicaTask,
    ) -> Result<(
        bool, /* ack current */
        bool, /* immediately step next tick */
    )> {
        let schema = self.shared.schema()?;

        let group = task.group;
        let group_desc = schema.get_group(group).await?;
        if group_desc.is_none() {
            warn!(
                group = group,
                "group not found abort reallocate replica task."
            );
            return Ok((true, false));
        }
        let group_desc = group_desc.unwrap();

        let mut leader_state = None;
        for repl in &group_desc.replicas {
            if let Some(replica_state) = schema.get_replica_state(group_desc.id, repl.id).await? {
                if matches!(
                    RaftRole::from_i32(replica_state.role).unwrap(),
                    RaftRole::Leader
                ) {
                    leader_state = Some(replica_state);
                    break;
                }
            }
        }
        if leader_state.is_none() {
            return Err(crate::Error::AbortScheduleTask(
                "shed leader replica cancelled due to no group leader",
            ));
        }
        let leader_state = leader_state.unwrap();

        if leader_state.node_id == task.src_node {
            let transferee = group_desc
                .replicas
                .iter()
                .find(|r| r.node_id != task.src_node)
                .unwrap();
            let r = self
                .try_shed_leader_before_remove(
                    group_desc.to_owned(),
                    leader_state.to_owned(),
                    transferee.id,
                )
                .await;
            match r {
                Ok(_) => {}
                Err(crate::Error::AbortScheduleTask(_)) => return Ok((true, false)),
                Err(crate::Error::EpochNotMatch(new_group)) => {
                    warn!(group = group, replica = transferee.id, new_group = ?new_group, "shed leader meet epoch not match, abort task and retry allocator");
                    return Ok((true, false));
                }
                Err(err) => {
                    warn!(group = group, replica = transferee.id, err = ?err, "shed leader in source replica fail, retry in next tick");
                    metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc();
                    return Err(err);
                }
            };
        }

        info!(
            group = group,
            src_node = task.src_node,
            dest_node = task.dest_node,
            "start move replica"
        );
        let src_replica = group_desc
            .replicas
            .iter()
            .find(|r| r.node_id == task.src_node);
        if src_replica.is_none() {
            warn!(
                group = group,
                dest_node = task.dest_node,
                "target replica not found abort reallocate replica task."
            );
            return Ok((true, false));
        }
        let src_replica = src_replica.unwrap();
        let next_replica = schema.next_replica_id().await?;
        match self
            .try_move_replica(
                group_desc.to_owned(),
                (leader_state.replica_id, leader_state.term),
                ReplicaDesc {
                    id: next_replica,
                    node_id: task.dest_node,
                    role: ReplicaRole::Voter as i32,
                },
                src_replica.to_owned(),
            )
            .await
        {
            Ok(schedule_state) => {
                let pdelta_src = self.ongoing_stats.get_node_delta(task.src_node);
                let pdelta_target = self.ongoing_stats.get_node_delta(task.dest_node);
                self.ongoing_stats.handle_update(&[schedule_state], None);
                let delta_src = self.ongoing_stats.get_node_delta(task.src_node);
                let delta_target = self.ongoing_stats.get_node_delta(task.dest_node);
                info!(
                    "update handle: src_node: {}({}:{}) -> dest_node: {}({}:{})",
                    task.src_node,
                    pdelta_src.replica_count,
                    delta_src.replica_count,
                    task.dest_node,
                    pdelta_target.replica_count,
                    delta_target.replica_count,
                );
                Ok((true, false))
            }
            Err(crate::Error::AlreadyExists(_)) | Err(crate::Error::EpochNotMatch(_)) => {
                warn!(
                    group = group,
                    src_node = task.src_node,
                    dest_node = task.dest_node,
                    "move replica task aborted due to replica already changed"
                );
                Ok((true, false))
            }
            Err(err) => {
                warn!(
                    group = group,
                    src_node = task.src_node,
                    dest_node = task.dest_node,
                    err = ?err,
                    "move replica meet error and retry later"
                );
                metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc();
                Err(err)
            }
        }
    }

    async fn handle_migrate_shard(
        &self,
        task: &mut MigrateShardTask,
    ) -> Result<(
        bool, /* ack current */
        bool, /* immediately step next tick */
    )> {
        info!(
            shard = task.shard,
            src_group = task.src_group,
            dest_group = task.dest_group,
            "start migrate shard"
        );
        let r = self
            .try_migrate_shard(task.src_group, task.dest_group, task.shard)
            .await;
        match r {
            Ok(_) => Ok((true, false)),
            Err(crate::Error::AbortScheduleTask(reason)) => {
                warn!(
                    shard = task.shard,
                    src_group = task.src_group,
                    dest_group = task.dest_group,
                    reason = reason,
                    "abort migrate shard"
                );
                Ok((true, false))
            }
            Err(err) => {
                warn!(shard = task.shard, src_group = task.src_group, dest_group = task.dest_group, err = ?&err, "migrate shard fail, retry later");
                Err(err)
            }
        }
    }

    async fn handle_transfer_leader(
        &self,
        task: &mut TransferGroupLeaderTask,
    ) -> Result<(
        bool, /* ack current */
        bool, /* immediately step next tick */
    )> {
        let schema = self.shared.schema()?;
        let group_desc = schema.get_group(task.group).await?;
        if group_desc.is_none() {
            warn!(
                group = task.group,
                epoch = task.epoch,
                "transfer group not found, abort transfer task"
            );
            return Ok((true, false));
        }
        let group_desc = group_desc.unwrap();
        if group_desc.epoch != task.epoch {
            warn!(
                group = task.group,
                "transfer group meet epoch not match, abort transfer task"
            );
            return Ok((true, false));
        }
        let leader_replica = group_desc
            .replicas
            .iter()
            .find(|r| r.node_id == task.src_node);
        if leader_replica.is_none() {
            warn!(
                group = task.group,
                epoch = task.epoch,
                "transfer src replica not found, abort transfer task"
            );
            return Ok((true, false));
        }
        let leader_state = schema
            .get_replica_state(task.group, leader_replica.as_ref().unwrap().id)
            .await?;
        if leader_state.is_none() {
            warn!(
                group = task.group,
                epoch = task.epoch,
                "transfer src replica not found, abort transfer task"
            );
            return Ok((true, false));
        }
        let replica_state = leader_state.unwrap();
        let target_replica = group_desc
            .replicas
            .iter()
            .cloned()
            .find(|r| r.node_id == task.dest_node);
        if target_replica.is_none() {
            warn!(
                group = task.group,
                epoch = task.epoch,
                "transfer target replica not found, abort transfer task"
            );
            return Ok((true, false));
        }
        match self
            .try_transfer_leader(
                group_desc,
                (replica_state.replica_id, replica_state.term),
                target_replica.as_ref().unwrap().id,
            )
            .await
        {
            Ok(_) => {}
            Err(crate::Error::EpochNotMatch(new_group)) => {
                warn!(group = task.group, epoch = task.epoch, new_group = ?&new_group, "transfer target meet epoch not match, abort transfer task");
                return Ok((true, false));
            }
            Err(err) => {
                error!(group = task.group, epoch = task.epoch, err = ?&err, "transfer group leader fail");
                return Err(err);
            }
        }
        self.heartbeat_queue
            .try_schedule(
                vec![
                    HeartbeatTask {
                        node_id: task.dest_node,
                    },
                    HeartbeatTask {
                        node_id: task.src_node,
                    },
                ],
                Instant::now(),
            )
            .await;
        Ok((true, true))
    }

    async fn handle_shed_leader(
        &self,
        shed: &mut ShedLeaderTask,
    ) -> Result<(
        bool, /* ack current */
        bool, /* immediately step next tick */
    )> {
        let node = shed.node_id;
        loop {
            let schema = self.shared.schema()?;

            if let Some(desc) = schema.get_node(node).await? {
                if desc.status != NodeStatus::Draining as i32 {
                    warn!(node = node, "shed leader task cancelled");
                    break;
                }
            }

            let leader_states = schema
                .list_replica_state()
                .await?
                .into_iter()
                .filter(|r| r.node_id == node && r.role == RaftRole::Leader as i32)
                .collect::<Vec<_>>();

            // exit when all leader move-out
            // also change node status to Drained
            if leader_states.is_empty() {
                if let Some(mut desc) = schema.get_node(node).await? {
                    if desc.status == NodeStatus::Draining as i32 {
                        desc.status = NodeStatus::Drained as i32;
                        schema.update_node(desc).await?; // TODO: cas
                    }
                }
                break;
            }

            for leader_state in &leader_states {
                let group_id = leader_state.group_id;
                if let Some(group) = schema.get_group(group_id).await? {
                    let mut target_replica = None;
                    for r in &group.replicas {
                        if r.id == leader_state.replica_id {
                            continue;
                        }
                        let target_node = schema.get_node(r.node_id).await?;
                        if target_node.is_none() {
                            continue;
                        }
                        if target_node.as_ref().unwrap().status != NodeStatus::Active as i32 {
                            continue;
                        }
                        target_replica = Some(r.to_owned())
                    }
                    if let Some(target_replica) = target_replica {
                        self.try_transfer_leader(
                            group,
                            (leader_state.replica_id, leader_state.term),
                            target_replica.id,
                        )
                        .await?;
                    } else {
                        warn!(
                            node = node,
                            group = group_id,
                            src_replica = leader_state.replica_id,
                            "shed leader from node fail due to no suitable target replica."
                        );
                        metrics::RECONCILE_RETRY_TASK_TOTAL.shed_group_leaders.inc();
                    }
                }
            }
        }

        Ok((true, true))
    }

    async fn handle_shed_root(
        &self,
        task: &mut ShedRootLeaderTask,
    ) -> Result<(
        bool, /* ack current */
        bool, /* immediately step next tick */
    )> {
        let node = task.node_id;
        let schema = self.shared.schema()?;
        let root_group = schema.get_group(ROOT_GROUP_ID).await?.unwrap();
        let mut target = None;
        let mut current = None;
        for r in &root_group.replicas {
            if r.node_id == node {
                current = Some(r.to_owned());
                continue;
            }
            let target_node = schema.get_node(r.node_id).await?;
            if target_node.is_none() {
                continue;
            }
            if target_node.as_ref().unwrap().status != NodeStatus::Active as i32 {
                continue;
            }
            target = Some(r.to_owned())
        }
        if current.is_some() && target.is_some() {
            let leader_state = schema
                .get_replica_state(ROOT_GROUP_ID, current.unwrap().id)
                .await?;
            if let Some(leader_state) = leader_state {
                if matches!(
                    RaftRole::from_i32(leader_state.role).unwrap(),
                    RaftRole::Leader
                ) {
                    self.try_transfer_leader(
                        root_group,
                        (leader_state.replica_id, leader_state.term),
                        target.unwrap().id,
                    )
                    .await?
                }
            }
        }
        Ok((true, false))
    }
}

impl ScheduleContext {
    async fn try_shed_leader_before_remove(
        &self,
        group: GroupDesc,
        leader_state: ReplicaState,
        target_replica: u64,
    ) -> Result<()> {
        info!(
            group = group.id,
            replica = target_replica,
            "attempt remove leader replica, so transfer leader to {}",
            target_replica,
        );
        self.try_transfer_leader(
            group,
            (leader_state.replica_id, leader_state.term),
            target_replica,
        )
        .await?;
        Ok(())
    }

    async fn try_move_replica(
        &self,
        group: GroupDesc,
        leader_state: (u64 /* id */, u64 /* term */),
        incoming_replica: ReplicaDesc,
        outgoing_replica: ReplicaDesc,
    ) -> Result<ScheduleState> {
        let mut group_client = GroupClient::new(
            RouterGroupState {
                id: group.id,
                epoch: group.epoch,
                leader_state: Some(leader_state),
                replicas: group
                    .replicas
                    .iter()
                    .map(|g| (g.id, g.to_owned()))
                    .collect::<HashMap<_, _>>(),
            },
            self.shared.provider.router.clone(),
            self.shared.provider.conn_manager.clone(),
        );
        let current_state = group_client
            .move_replicas(vec![incoming_replica], vec![outgoing_replica])
            .await?;
        Ok(current_state)
    }

    async fn try_transfer_leader(
        &self,
        group: GroupDesc,
        leader_state: (u64 /* id */, u64 /* term */),
        target_replica: u64,
    ) -> Result<()> {
        let group_state = RouterGroupState {
            id: group.id,
            epoch: group.epoch,
            leader_state: Some(leader_state),
            replicas: group
                .replicas
                .iter()
                .map(|g| (g.id, g.to_owned()))
                .collect::<HashMap<_, _>>(),
        };
        let mut group_client = GroupClient::new(
            group_state,
            self.shared.provider.router.clone(),
            self.shared.provider.conn_manager.clone(),
        );
        group_client.transfer_leader(target_replica).await?;
        Ok(())
    }

    async fn try_migrate_shard(&self, src_group: u64, target_group: u64, shard: u64) -> Result<()> {
        let schema = self.shared.schema()?;
        let src_group =
            schema
                .get_group(src_group)
                .await?
                .ok_or(crate::Error::AbortScheduleTask(
                    "migrate source group has be destroyed",
                ))?;
        let shard_desc = src_group.shards.iter().find(|s| s.id == shard).ok_or(
            crate::Error::AbortScheduleTask("migrate shard has be moved out"),
        )?;

        let mut group_client = GroupClient::lazy(
            target_group,
            self.shared.provider.router.clone(),
            self.shared.provider.conn_manager.clone(),
        );
        group_client
            .accept_shard(src_group.id, src_group.epoch, shard_desc)
            .await?;
        // TODO: handle src_group epoch not match?
        Ok(())
    }
}
