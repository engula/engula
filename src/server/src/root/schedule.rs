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
use engula_client::NodeClient;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use super::{
    allocator::{GroupAction, LeaderAction, ReplicaAction, ReplicaRoleAction, ShardAction},
    RootShared,
};
use crate::{
    bootstrap::INITIAL_EPOCH,
    client::GroupClient,
    root::{
        allocator::{Allocator, SysAllocSource},
        Schema,
    },
    serverpb::v1::{reconcile_task::Task, *},
    Result,
};

pub struct ReconcileScheduler {
    ctx: ScheduleContext,
    cfg: ScheduleConfig,

    tasks: Mutex<LinkedList<ReconcileTask>>,
}

pub struct ScheduleContext {
    shared: Arc<RootShared>,
    alloc: Arc<Allocator<SysAllocSource>>,
}

pub struct ScheduleConfig {
    max_create_group_retry_before_rollback: u64,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            max_create_group_retry_before_rollback: 10,
        }
    }
}

impl ReconcileScheduler {
    pub fn new(ctx: ScheduleContext) -> Self {
        Self {
            ctx,
            tasks: Default::default(),
            cfg: Default::default(),
        }
    }

    pub async fn step_one(&self) {
        let cr = self.check(1).await; // TODO: take care self.tasks then can give more > 1 value here.
        if cr.is_ok() && cr.unwrap() {
            self.advance_tasks().await;
        }
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

    pub async fn check(&self, max_try_per_tick: u64) -> Result<bool> {
        let group_action = self.ctx.alloc.compute_group_action().await?;
        if let GroupAction::Add(cnt) = group_action {
            for _ in 0..cnt {
                self.setup_task(ReconcileTask {
                    task: Some(reconcile_task::Task::CreateGroup(CreateGroupTask {
                        request_replica_cnt: cnt as u64,
                        step: GroupTaskStep::GroupInit as i32,
                        ..Default::default()
                    })),
                })
                .await;
            }
            return Ok(true);
        }

        let mut ractions = self.comput_replica_role_action().await?;
        let mut sactions = self.ctx.alloc.compute_shard_action().await?;
        for _ in 0..max_try_per_tick {
            if ractions.is_empty() && sactions.is_empty() {
                break;
            }

            for action in ractions {
                match action {
                    ReplicaRoleAction::Replica(ReplicaAction::Migrate(action)) => {
                        self.setup_task(ReconcileTask {
                            task: Some(reconcile_task::Task::ReallocateReplica(
                                ReallocateReplicaTask {
                                    group: action.group,
                                    src_node: action.source_node,
                                    src_replica: action.source_replica,
                                    dest_node: Some(action.target_node),
                                    dest_replica: None,
                                    step: ReallocateReplicaTaskStep::CreatingDestReplica as i32,
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
                                    target_replica: action.target_replica,
                                },
                            )),
                        })
                        .await;
                    }
                    _ => {}
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

            ractions = self.comput_replica_role_action().await?;
            sactions = self.ctx.alloc.compute_shard_action().await?;
        }

        Ok(!self.is_empty().await)
    }

    pub async fn comput_replica_role_action(&self) -> Result<Vec<ReplicaRoleAction>> {
        let mut actions = Vec::new();
        let replica_actions = self.ctx.alloc.compute_replica_action().await?;
        actions.extend_from_slice(
            &replica_actions
                .iter()
                .cloned()
                .map(ReplicaRoleAction::Replica)
                .collect::<Vec<_>>(),
        );
        let leader_actions = self.ctx.alloc.compute_leader_action().await?;
        actions.extend_from_slice(
            &leader_actions
                .iter()
                .cloned()
                .map(ReplicaRoleAction::Leader)
                .collect::<Vec<_>>(),
        );
        Ok(actions)
    }
}

impl ReconcileScheduler {
    async fn advance_tasks(&self) {
        let mut task = self.tasks.lock().await;
        let mut cursor = task.cursor_front_mut();
        while let Some(task) = cursor.current() {
            let rs = self.ctx.handle_task(&self.cfg, task).await;
            if rs.is_ok() && rs.unwrap() {
                cursor.remove_current();
            } else {
                cursor.move_next();
            }
        }
    }
}

impl ScheduleContext {
    pub(crate) fn new(shared: Arc<RootShared>, alloc: Arc<Allocator<SysAllocSource>>) -> Self {
        Self { shared, alloc }
    }

    pub async fn handle_task(
        &self,
        cfg: &ScheduleConfig,
        task: &mut ReconcileTask,
    ) -> Result<bool> {
        info!(task=?task, "handle reconcile task");
        match task.task.as_mut().unwrap() {
            Task::CreateGroup(create_group) => self.handle_create_group(cfg, create_group).await,
            Task::ReallocateReplica(reallocate_replica) => {
                self.handle_reallocate_replica(reallocate_replica).await
            }
            Task::MigrateShard(migrate_shard) => self.handle_migrate_shard(migrate_shard).await,
            Task::TransferGroupLeader(transfer_leader) => {
                self.handle_transfer_leader(transfer_leader).await
            }
            Task::CreateCollectionShards(create_collection_shards) => {
                self.handle_create_collection_shards(create_collection_shards)
                    .await
            }
        }
    }

    async fn handle_create_group(
        &self,
        cfg: &ScheduleConfig,
        task: &mut CreateGroupTask,
    ) -> Result<bool /* finish */> {
        loop {
            match GroupTaskStep::from_i32(task.step).unwrap() {
                GroupTaskStep::GroupInit => {
                    let schema = self.shared.schema()?;
                    let nodes = self
                        .alloc
                        .allocate_group_replica(vec![], task.request_replica_cnt as usize)
                        .await?;
                    let group_id = schema.next_group_id().await?;
                    let mut replicas = Vec::new();
                    for n in &nodes {
                        let replica_id = schema.next_replica_id().await?;
                        replicas.push(ReplicaDesc {
                            id: replica_id,
                            node_id: n.id,
                            role: ReplicaRole::Voter.into(),
                        });
                    }
                    let group_desc = GroupDesc {
                        id: group_id,
                        epoch: INITIAL_EPOCH,
                        shards: vec![],
                        replicas,
                    };
                    {
                        task.group_desc = Some(group_desc);
                        task.wait_create = nodes;
                        task.step = GroupTaskStep::GroupCreating.into();
                    }
                }
                GroupTaskStep::GroupCreating => {
                    let mut wait_create = task.wait_create.to_owned();
                    let group_desc = task.group_desc.as_ref().unwrap().to_owned();
                    let mut undo = Vec::new();
                    loop {
                        let n = wait_create.pop();
                        if n.is_none() {
                            break;
                        }
                        let n = n.unwrap();
                        let replica = group_desc
                            .replicas
                            .iter()
                            .find(|r| r.node_id == n.id)
                            .unwrap();
                        if let Err(err) = self
                            .try_create_replica(&n.addr, &replica.id, group_desc.to_owned())
                            .await
                        {
                            let retried = task.create_retry;
                            if retried < cfg.max_create_group_retry_before_rollback {
                                warn!(node=n.id, replica=replica.id, group=group_desc.id, retried = retried, err = ?err, "create replica for new group error, retry in next");
                                {
                                    task.create_retry += 1;
                                }
                            } else {
                                warn!(node=n.id, replica=replica.id, group=group_desc.id, err = ?err, "create replica for new group error, start rollback");
                                {
                                    task.step = GroupTaskStep::GroupRollbacking.into();
                                }
                            };
                            continue;
                        }
                        undo.push(replica.to_owned());
                        {
                            task.wait_create = wait_create.to_owned();
                            task.wait_cleanup = undo.to_owned();
                        }
                    }
                    {
                        task.step = GroupTaskStep::GroupFinish.into();
                    }
                }
                GroupTaskStep::GroupRollbacking => {
                    let mut wait_clean = task.wait_cleanup.to_owned();
                    loop {
                        let r = wait_clean.pop();
                        if r.is_none() {
                            break;
                        }
                        let r = r.unwrap();
                        if let Err(err) = self.try_remove_replica(r.id).await {
                            error!(err = ?err, replica=r.id, "rollback temp replica of new group fail and retry later");
                            {
                                task.wait_cleanup = wait_clean.to_owned();
                            }
                            return Err(err);
                        }
                    }
                    {
                        task.step = GroupTaskStep::GroupAbort.into();
                    }
                }
                GroupTaskStep::GroupFinish | GroupTaskStep::GroupAbort => return Ok(true),
            }
        }
    }

    async fn handle_reallocate_replica(
        &self,
        task: &mut ReallocateReplicaTask,
    ) -> Result<bool /* done */> {
        loop {
            match ReallocateReplicaTaskStep::from_i32(task.step).unwrap() {
                ReallocateReplicaTaskStep::CreatingDestReplica => {
                    let schema = self.shared.schema()?;
                    let node_id = task.dest_node.as_ref().unwrap().id;
                    let r = self.try_add_replica(schema, task.group, node_id).await;
                    if let Err(err) = &r {
                        error!(group = task.group, node = node_id, err = ?err, "create replica for transfer dest replica error, abort reallocate");
                        {
                            task.step = ReallocateReplicaTaskStep::ReallocateAbort.into();
                        }
                    } else {
                        let dest_replica = r.unwrap();
                        {
                            task.dest_replica = Some(dest_replica);
                            task.step = ReallocateReplicaTaskStep::AddDestLearner.into();
                        }
                    };
                }
                ReallocateReplicaTaskStep::AddDestLearner => {
                    let group = task.group;
                    let replica = task.dest_replica.as_ref().unwrap();
                    let dest_node = task.dest_node.as_ref().unwrap().id;
                    let r = self.try_add_learner(group, replica.id, dest_node).await;
                    if let Err(err) = &r {
                        warn!(group = group, replica = replica.id, err = ?err, "add replica to group as learner fail, retry in next tick");
                        continue;
                    }
                    {
                        task.step = ReallocateReplicaTaskStep::ReplaceDestVoter.into();
                    }
                }
                ReallocateReplicaTaskStep::ReplaceDestVoter => {
                    let group = task.group;
                    let replica = task.dest_replica.as_ref().unwrap();
                    let dest_node = task.dest_node.as_ref().unwrap().id;
                    let r = self.try_replace_voter(group, replica.id, dest_node).await;
                    if let Err(err) = &r {
                        warn!(group = group, replica = replica.id, err = ?err, "replace learner to voter fail, retry in next tick");
                        continue;
                    }
                    {
                        task.step = ReallocateReplicaTaskStep::ShedSourceLeader.into();
                    }
                }
                ReallocateReplicaTaskStep::ShedSourceLeader => {
                    let group = task.group;
                    let replica = task.src_replica;
                    let r = self.try_shed_leader(group, replica).await;
                    if let Err(err) = &r {
                        warn!(group = group, replica = replica, err = ?err, "shed leader in source replica fail, retry in next tick");
                        continue;
                    }
                    {
                        task.step = ReallocateReplicaTaskStep::RemoveSourceMembership.into();
                    }
                }
                ReallocateReplicaTaskStep::RemoveSourceMembership => {
                    let group = task.group;
                    let replica = task.src_replica;
                    let r = self.try_remove_membership(group, replica).await;
                    if let Err(err) = &r {
                        warn!(group = group, replica = replica, err = ?err, "remove source replica from group fail, retry in next tick");
                        continue;
                    }
                    {
                        task.step = ReallocateReplicaTaskStep::RemoveSourceReplica.into();
                    }
                }
                ReallocateReplicaTaskStep::RemoveSourceReplica => {
                    let replica = task.src_replica;
                    let r = self.try_remove_replica(replica.to_owned()).await;
                    if let Err(err) = &r {
                        warn!(group = task.group, replica = replica, err = ?err, "remove source replica from group fail, retry in next tick");
                        continue;
                    }
                    {
                        task.step = ReallocateReplicaTaskStep::ReallocateFinish.into();
                    }
                }
                ReallocateReplicaTaskStep::ReallocateFinish
                | ReallocateReplicaTaskStep::ReallocateAbort => return Ok(true),
            }
        }
    }

    async fn handle_migrate_shard(&self, task: &mut MigrateShardTask) -> Result<bool> {
        let r = self
            .try_migrate_shard(task.src_group, task.dest_group, task.shard)
            .await;
        if let Err(err) = r {
            error!(shard = task.shard, src_group = task.src_group, dest_group = task.dest_group, err = ?&err, "migrate shard fail");
            return Err(err);
        }
        Ok(true)
    }

    async fn handle_transfer_leader(&self, task: &mut TransferGroupLeaderTask) -> Result<bool> {
        let r = self
            .try_transfer_leader(task.group, task.target_replica)
            .await;
        if let Err(err) = r {
            error!(group = task.group, dest_replica = task.target_replica, err = ?&err, "transfer group leader fail");
            return Err(err);
        }
        Ok(true)
    }

    async fn handle_create_collection_shards(
        &self,
        task: &mut CreateCollectionShards,
    ) -> Result<bool> {
        loop {
            match CreateCollectionShardStep::from_i32(task.step).unwrap() {
                CreateCollectionShardStep::CollectionCreating => {
                    let mut wait_cleanup = Vec::new();
                    let mut wait_create = task.wait_create.to_owned();
                    loop {
                        let mut desc = wait_create.pop();
                        if desc.is_none() {
                            break;
                        }
                        let group_shards = desc.take().unwrap();
                        // TODO: maybe batch request support refresh epoch in server-side to avoid
                        // loop?
                        for desc in group_shards.to_owned().shards {
                            if let Err(err) = self.try_create_shard(group_shards.group, &desc).await
                            {
                                error!(group=group_shards.group, shard=desc.id, err=?err, "create collection shard error and try to rollback");
                                {
                                    task.step =
                                        CreateCollectionShardStep::CollectionRollbaking.into();
                                }
                                return Err(err);
                            }
                            wait_cleanup.push(desc.to_owned());
                            {
                                task.wait_create = wait_create.to_owned();
                                task.wait_cleanup = wait_cleanup.to_owned();
                            }
                        }
                    }
                    {
                        task.step = CreateCollectionShardStep::CollectionFinish.into();
                    }
                }
                CreateCollectionShardStep::CollectionRollbaking => {
                    // TODO: remove the shard in wait_cleanup.
                    task.step = CreateCollectionShardStep::CollectionAbort.into();
                }
                CreateCollectionShardStep::CollectionFinish
                | CreateCollectionShardStep::CollectionAbort => return Ok(true),
            }
        }
    }
}

impl ScheduleContext {
    async fn get_node_client(&self, addr: String) -> Result<NodeClient> {
        let client = self
            .shared
            .provider
            .conn_manager
            .get_node_client(addr)
            .await?;
        Ok(client)
    }

    async fn get_group_leader(&self, group_id: u64) -> Result<GroupDesc> {
        let schema = self.shared.schema()?;
        let group = schema
            .get_group(group_id)
            .await?
            .ok_or(crate::Error::GroupNotFound(group_id))?;
        Ok(group)
    }

    async fn try_create_replica(
        &self,
        addr: &str,
        replica_id: &u64,
        group: GroupDesc,
    ) -> Result<()> {
        self.get_node_client(addr.to_owned())
            .await?
            .create_replica(replica_id.to_owned(), group)
            .await?;
        Ok(())
    }

    async fn try_add_replica(
        &self,
        schema: Arc<Schema>,
        group_id: u64,
        target_node_id: u64,
    ) -> Result<ReplicaDesc> {
        let new_replica = schema.next_replica_id().await?;
        let target_node = schema
            .get_node(target_node_id.to_owned())
            .await?
            .ok_or(crate::Error::GroupNotFound(group_id))?;
        self.get_node_client(target_node.addr.clone())
            .await?
            .create_replica(
                new_replica.to_owned(),
                GroupDesc {
                    id: group_id,
                    ..Default::default()
                },
            )
            .await?;
        Ok(ReplicaDesc {
            id: new_replica,
            node_id: target_node.id,
            role: ReplicaRole::Voter.into(),
        })
    }

    async fn try_add_learner(&self, group_id: u64, replica_id: u64, node_id: u64) -> Result<()> {
        let mut group_client = GroupClient::new(group_id, self.shared.provider.to_owned());
        group_client.add_learner(replica_id, node_id).await
    }

    async fn try_replace_voter(&self, group_id: u64, replica_id: u64, node_id: u64) -> Result<()> {
        let mut group_client = GroupClient::new(group_id, self.shared.provider.to_owned());
        group_client.add_replica(replica_id, node_id).await
    }

    async fn try_shed_leader(&self, group_id: u64, remove_replica: u64) -> Result<()> {
        let schema = self.shared.schema()?;

        let replica_state = schema
            .get_replica_state(group_id, remove_replica)
            .await?
            .ok_or(crate::Error::GroupNotFound(group_id))?;

        if replica_state.role != RaftRole::Leader as i32 {
            return Ok(());
        }

        let group = self.get_group_leader(group_id).await?;
        if let Some(target_replica) = group.replicas.iter().find(|e| e.id != remove_replica) {
            // TODO: find least-leader node.
            info!(
                group = group.id,
                replica = remove_replica,
                "attemp remove leader replica, so transfer leader to {} in node {}",
                target_replica.id,
                target_replica.node_id,
            );
            self.try_transfer_leader(group_id, target_replica.id)
                .await?;
        }
        Err(crate::Error::GroupNotFound(group_id))
    }

    async fn try_remove_membership(&self, group: u64, remove_replica: u64) -> Result<()> {
        let mut group_client = GroupClient::new(group, self.shared.provider.to_owned());
        group_client.remove_group_replica(remove_replica).await
    }

    async fn try_remove_replica(&self, _r: u64) -> Result<()> {
        // TODO: call remove replica.
        Ok(())
    }

    async fn try_transfer_leader(&self, group: u64, target_replica: u64) -> Result<()> {
        let mut group_client = GroupClient::new(group, self.shared.provider.to_owned());
        group_client.transfer_leader(target_replica).await
    }

    async fn try_migrate_shard(&self, src_group: u64, target_group: u64, shard: u64) -> Result<()> {
        let src_group = self.get_group_leader(src_group).await?;
        let shard_desc = src_group
            .shards
            .iter()
            .find(|s| s.id == shard)
            .ok_or(crate::Error::GroupNotFound(src_group.id))?;

        let mut group_client = GroupClient::new(target_group, self.shared.provider.to_owned());
        group_client
            .migrate_shard(src_group.id, src_group.epoch, shard_desc)
            .await
        // TODO: handle src_group epocho not match?
    }

    async fn try_create_shard(&self, group_id: u64, desc: &ShardDesc) -> Result<()> {
        let mut group_client = GroupClient::new(group_id, self.shared.provider.to_owned());
        group_client.create_shard(desc).await
    }
}
