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
use tokio::sync::Mutex;
use tracing::{info, warn};

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
    pub shared: Arc<RootShared>,
    pub alloc: Arc<Allocator<SysAllocSource>>,
    pub leader_balancer: Arc<LeaderBalancer<SysAllocSource>>,
    pub replica_balancer: Arc<ReplicaBalancer<SysAllocSource>>,
    pub shard_balancer: Arc<ShardBalancer<SysAllocSource>>,
    pub heartbeat_queue: Arc<HeartbeatQueue>,
    pub jobs: Arc<Jobs>,
    pub cfg: RootConfig,
}

impl ReconcileScheduler {
    pub fn new(ctx: ScheduleContext) -> Self {
        Self {
            ctx,
            tasks: Default::default(),
        }
    }

    pub async fn step_one(&self) -> Duration {
        info!("step one reconcile");

        let interval = Duration::from_secs(4); //self.ctx.cfg.schedule_interval_sec

        // 1. try reconcile group for node and execute as bg job.
        let gr = self.try_reconcile_group().await;
        match gr {
            Ok(wait_next_tick) => {
                if wait_next_tick {
                    // balancing group and wait for a tick to do following balance.
                    return interval;
                }
            }
            Err(err) => {
                warn!(err = ?err, "reconcile group meet error and retry later");
                return interval;
            }
        }

        info!("group balanced");

        // 2. try leader balance for exist replica.
        let lbr = self.ctx.leader_balancer.balance_leader().await;
        let _maybe_reallocate_nodes = match lbr {
            Ok(nodes) => nodes,
            Err(err) => {
                warn!(err = ?err, "balance leader meet error and retry later");
                return interval;
            }
        };

        info!("leader balanced");

        let rr = self
            .ctx
            .replica_balancer
            .balance_replica(&ReplicaCountPolicy::default())
            .await;
        match rr {
            Ok(_) => {}
            Err(err) => {
                warn!(err = ?err, "balance replica meet error and retry later");
                return interval;
            }
        }

        info!("replica balanced");

        let sr = self.ctx.shard_balancer.balance_shard().await;
        match sr {
            Ok(_) => {}
            Err(err) => {
                warn!(err = ?err, "balance shard meet error and retry later");
                return interval;
            }
        }

        info!("shard balanced");

        self.advance_tasks().await;

        interval
    }

    pub async fn wait_heartbeat_tick(&self) {
        self.ctx.heartbeat_queue.wait_one_heartbeat_tick().await
    }

    pub async fn setup_task(&self, task: ReconcileTask) {
        let mut tasks = self.tasks.lock().await;
        tasks.push_back(task.to_owned());
        info!(len = tasks.len(), task=?task, "setup new reconcile task")
    }
}

impl ReconcileScheduler {
    pub async fn need_reconcile(&self) -> Result<bool> {
        let group_action = self.ctx.alloc.compute_group_action().await?;
        if matches!(group_action, GroupAction::Add(_)) {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .cluster_groups
                .set(0);
            return Ok(true);
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .cluster_groups
            .set(1);

        if self.ctx.leader_balancer.need_balance().await? {
            return Ok(true);
        }

        if self
            .ctx
            .replica_balancer
            .need_balance(&ReplicaCountPolicy::default())
            .await?
        {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .node_replica_count
                .set(0);
            return Ok(true);
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .node_replica_count
            .set(1);

        if self.ctx.shard_balancer.need_balance().await? {
            metrics::RECONCILE_ALREADY_BALANCED_INFO
                .group_shard_count
                .set(0);
            return Ok(true);
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .group_shard_count
            .set(1);
        Ok(false)
    }

    pub async fn try_reconcile_group(&self) -> Result<bool /* wait next tick */> {
        let group_action = self.ctx.alloc.compute_group_action().await?;
        if let GroupAction::Add(cnt) = group_action {
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
        Ok(false)
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
    pub async fn handle_task(
        &self,
        task: &mut ReconcileTask,
    ) -> Result<(
        bool, /* ack current */
        bool, /* immediately step next tick */
    )> {
        info!(task=?task, "handle reconcile task");
        match task.task.as_mut().unwrap() {
            Task::ShedLeader(shed_leader) => self.handle_shed_leader(shed_leader).await,
            Task::ShedRoot(shed_root) => self.handle_shed_root(shed_root).await,
            Task::ReallocateReplica(_) | Task::MigrateShard(_) | Task::TransferGroupLeader(_) => {
                unreachable!()
            }
        }
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
}
