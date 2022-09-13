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
    collections::HashSet,
    sync::{atomic, Arc, Mutex},
    task::{Poll, Waker},
};

use engula_api::server::v1::{GroupDesc, ReplicaDesc, ReplicaRole, RootDesc, ShardDesc};
use engula_client::GroupClient;
use futures::future::poll_fn;
use prometheus::HistogramTimer;
use tokio::time::Instant;
use tracing::{error, info, warn};

use super::{allocator::*, HeartbeatQueue, HeartbeatTask, RootShared, Schema};
use crate::{
    bootstrap::INITIAL_EPOCH,
    root::metrics,
    serverpb::v1::{background_job::Job, *},
    Result,
};

pub struct Jobs {
    core: JobCore,
}

impl Jobs {
    pub fn new(
        root_shared: Arc<RootShared>,
        alloc: Arc<Allocator<SysAllocSource>>,
        heartbeat_queue: Arc<HeartbeatQueue>,
        shard_balancer: Arc<ShardBalancer<SysAllocSource>>,
    ) -> Self {
        Self {
            core: JobCore {
                root_shared,
                alloc,
                shard_balancer,
                heartbeat_queue,
                mem_jobs: Default::default(),
                res_locks: Default::default(),
                enable: Default::default(),
            },
        }
    }

    pub async fn submit(&self, job: BackgroundJob, wait_result: bool) -> Result<()> {
        self.core.check_root_leader()?;
        let job = self.core.append(job).await?;
        if wait_result {
            self.core.wait_and_check_result(&job.id).await?;
        }
        Ok(())
    }

    pub async fn wait_more_jobs(&self) {
        self.core.wait_more_jobs().await;
    }

    pub async fn advance_jobs(&self) -> Result<()> {
        let jobs = self.core.need_handle_jobs();
        for job in &jobs {
            self.handle_job(job).await?;
        }
        Ok(())
    }

    pub async fn on_step_leader(&self) -> Result<()> {
        self.core.recovery().await?;
        self.core.enable.store(true, atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn on_drop_leader(&self) {
        self.core.enable.store(false, atomic::Ordering::Relaxed);
        self.core.on_drop_leader()
    }

    async fn handle_job(&self, job: &BackgroundJob) -> Result<()> {
        info!("start background job: {job:?}");
        let r = match job.job.as_ref().unwrap() {
            background_job::Job::CreateCollection(create_collection) => {
                self.handle_create_collection(job, create_collection).await
            }
            background_job::Job::CreateOneGroup(create_group) => {
                self.handle_create_one_group(job, create_group).await
            }
            background_job::Job::PurgeCollection(purge_collection) => {
                self.handle_purge_collection(job, purge_collection).await
            }
            background_job::Job::PurgeDatabase(purge_database) => {
                self.handle_purge_database(job, purge_database).await
            }
        };
        info!("backgroud job: {job:?}, handle result: {r:?}");
        r
    }
}

impl Jobs {
    // handle create_collection.
    async fn handle_create_collection(
        &self,
        job: &BackgroundJob,
        create_collection: &CreateCollectionJob,
    ) -> Result<()> {
        let mut create_collection = create_collection.to_owned();
        loop {
            let status = CreateCollectionJobStatus::from_i32(create_collection.status).unwrap();
            let _timer = Self::record_create_collection_step(&status);
            match status {
                CreateCollectionJobStatus::CreateCollectionCreating => {
                    self.handle_wait_create_shard(job.id, &mut create_collection)
                        .await?;
                }
                CreateCollectionJobStatus::CreateCollectionRollbacking => {
                    self.handle_wait_cleanup_shard(job.id, &mut create_collection)
                        .await?;
                }
                CreateCollectionJobStatus::CreateCollectionWriteDesc => {
                    self.handle_write_desc(job.id, &mut create_collection)
                        .await?;
                }
                CreateCollectionJobStatus::CreateCollectionFinish
                | CreateCollectionJobStatus::CreateCollectionAbort => {
                    self.handle_finish_create_collection(job, create_collection)
                        .await?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_wait_create_shard(
        &self,
        job_id: u64,
        create_collection: &mut CreateCollectionJob,
    ) -> Result<()> {
        loop {
            let shard = create_collection.wait_create.pop();
            if shard.is_none() {
                break;
            }
            let shard = shard.unwrap();
            let groups = self.core.shard_balancer.allocate_shard(1).await?;
            if groups.is_empty() {
                return Err(crate::Error::ResourceExhausted("no engouth groups".into()));
            }
            let group = groups.first().unwrap();
            info!("try create shard at group {}", group.id);
            if let Err(err) = self.try_create_shard(group.id, &shard).await {
                error!(group=group.id, shard=shard.id, err=?err, "create collection shard error and try to rollback");
                create_collection.remark = format!("{err:?}");
                create_collection.wait_cleanup.push(shard);
                create_collection.status =
                    CreateCollectionJobStatus::CreateCollectionRollbacking as i32;
                self.save_create_collection(job_id, create_collection)
                    .await?;
                return Ok(());
            }
            create_collection.wait_cleanup.push(shard);
            self.save_create_collection(job_id, create_collection)
                .await?;
        }
        create_collection.status = CreateCollectionJobStatus::CreateCollectionWriteDesc as i32;
        self.save_create_collection(job_id, create_collection)
            .await?;
        Ok(())
    }

    async fn handle_write_desc(
        &self,
        job_id: u64,
        create_collection: &mut CreateCollectionJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        schema
            .create_collection(create_collection.desc.as_ref().unwrap().to_owned())
            .await?;
        create_collection.status = CreateCollectionJobStatus::CreateCollectionFinish as i32;
        self.save_create_collection(job_id, create_collection)
            .await?;
        Ok(())
    }

    async fn handle_wait_cleanup_shard(
        &self,
        job_id: u64,
        create_collection: &mut CreateCollectionJob,
    ) -> Result<()> {
        loop {
            let shard = create_collection.wait_cleanup.pop();
            if shard.is_none() {
                break;
            }
            let _ = shard; // TODO: delete shard.
            self.save_create_collection(job_id, create_collection)
                .await?;
        }
        create_collection.status = CreateCollectionJobStatus::CreateCollectionAbort as i32;
        self.save_create_collection(job_id, create_collection)
            .await?;
        Ok(())
    }

    async fn handle_finish_create_collection(
        &self,
        job: &BackgroundJob,
        create_collection: CreateCollectionJob,
    ) -> Result<()> {
        let mut job = job.to_owned();
        job.job = Some(background_job::Job::CreateCollection(create_collection));
        self.core.finish(job).await?;
        Ok(())
    }

    async fn save_create_collection(
        &self,
        job_id: u64,
        create_collection: &CreateCollectionJob,
    ) -> Result<()> {
        self.core
            .update(BackgroundJob {
                id: job_id,
                job: Some(background_job::Job::CreateCollection(
                    create_collection.to_owned(),
                )),
            })
            .await?;
        Ok(())
    }

    fn record_create_collection_step(step: &CreateCollectionJobStatus) -> Option<HistogramTimer> {
        match step {
            CreateCollectionJobStatus::CreateCollectionCreating => Some(
                metrics::RECONCILE_CREATE_COLLECTION_STEP_DURATION_SECONDS
                    .create
                    .start_timer(),
            ),
            CreateCollectionJobStatus::CreateCollectionRollbacking => Some(
                metrics::RECONCILE_CREATE_COLLECTION_STEP_DURATION_SECONDS
                    .rollback
                    .start_timer(),
            ),
            CreateCollectionJobStatus::CreateCollectionWriteDesc => Some(
                metrics::RECONCILE_CREATE_COLLECTION_STEP_DURATION_SECONDS
                    .write_desc
                    .start_timer(),
            ),
            CreateCollectionJobStatus::CreateCollectionFinish
            | CreateCollectionJobStatus::CreateCollectionAbort => Some(
                metrics::RECONCILE_CREATE_COLLECTION_STEP_DURATION_SECONDS
                    .finish
                    .start_timer(),
            ),
        }
    }
}

impl Jobs {
    // handle create_one_group
    async fn handle_create_one_group(
        &self,
        job: &BackgroundJob,
        create_group: &CreateOneGroupJob,
    ) -> Result<()> {
        let mut create_group = create_group.to_owned();
        loop {
            let status = CreateOneGroupStatus::from_i32(create_group.status).unwrap();
            let _timer = Self::record_create_group_step(&status);
            match status {
                CreateOneGroupStatus::CreateOneGroupInit => {
                    self.handle_init_create_group_replicas(job.id, &mut create_group)
                        .await?
                }
                CreateOneGroupStatus::CreateOneGroupCreating => {
                    self.handle_wait_create_group_replicas(job.id, &mut create_group)
                        .await?
                }
                CreateOneGroupStatus::CreateOneGroupRollbacking => {
                    self.handle_rollback_group_replicas(job.id, &mut create_group)
                        .await?
                }

                CreateOneGroupStatus::CreateOneGroupFinish
                | CreateOneGroupStatus::CreateOneGroupAbort => {
                    return self.handle_finish_create_group(job, create_group).await
                }
            }
        }
    }

    async fn check_is_already_meet_requirement(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
        schema: Arc<Schema>,
    ) -> Result<bool> {
        if create_group.group_desc.is_none() {
            return Ok(false);
        }
        let replicas = schema
            .group_replica_states(create_group.group_desc.as_ref().unwrap().id)
            .await?;
        if replicas.len() < create_group.request_replica_cnt as usize {
            return Ok(false);
        }
        warn!(
            group = create_group.group_desc.as_ref().unwrap().id,
            "cluster group count already meet requirement, so abort group creation."
        );
        create_group.status = CreateOneGroupStatus::CreateOneGroupRollbacking as i32;
        self.save_create_group(job_id, create_group).await?;
        Ok(true)
    }

    async fn handle_init_create_group_replicas(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        if self
            .check_is_already_meet_requirement(job_id, create_group, schema.to_owned())
            .await?
        {
            return Ok(());
        }
        let nodes = self
            .core
            .alloc
            .allocate_group_replica(vec![], create_group.request_replica_cnt as usize)
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
        create_group.group_desc = Some(group_desc);
        create_group.wait_create = nodes;
        create_group.status = CreateOneGroupStatus::CreateOneGroupCreating as i32;
        self.save_create_group(job_id, create_group).await
    }

    async fn handle_wait_create_group_replicas(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        if self
            .check_is_already_meet_requirement(job_id, create_group, schema)
            .await?
        {
            return Ok(());
        }
        let mut wait_create = create_group.wait_create.to_owned();
        let group_desc = create_group.group_desc.as_ref().unwrap().to_owned();
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
                let retried = create_group.create_retry;
                if retried < 20 {
                    warn!(node=n.id, replica=replica.id, group=group_desc.id, retried = retried, err = ?err, "create replica for new group error, retry in next");
                    metrics::RECONCILE_RETRY_TASK_TOTAL.create_group.inc();
                    create_group.create_retry += 1;
                } else {
                    warn!(node=n.id, replica=replica.id, group=group_desc.id, err = ?err, "create replica for new group error, start rollback");
                    create_group.status = CreateOneGroupStatus::CreateOneGroupRollbacking as i32;
                };
                self.save_create_group(job_id, create_group).await?;
                continue;
            }
            undo.push(replica.to_owned());
            create_group.wait_create = wait_create.to_owned();
            create_group.wait_cleanup = undo.to_owned();
            self.save_create_group(job_id, create_group).await?;
        }
        create_group.status = CreateOneGroupStatus::CreateOneGroupFinish as i32;
        self.save_create_group(job_id, create_group).await?;
        Ok(())
    }

    async fn handle_rollback_group_replicas(
        &self,
        job_id: u64,
        create_group: &mut CreateOneGroupJob,
    ) -> Result<()> {
        let mut wait_clean = create_group.wait_cleanup.to_owned();
        loop {
            let r = wait_clean.pop();
            if r.is_none() {
                break;
            }
            let group = create_group.group_desc.as_ref().unwrap().id;
            let r = r.unwrap();
            if let Err(err) = self.try_remove_replica(group, r.id).await {
                error!(err = ?err, replica=r.id, "rollback temp replica of new group fail and retry later");
                create_group.wait_cleanup = wait_clean.to_owned();
                self.save_create_group(job_id, create_group).await?;
                return Err(err);
            }
        }
        create_group.status = CreateOneGroupStatus::CreateOneGroupAbort as i32;
        self.save_create_group(job_id, create_group).await
    }

    async fn save_create_group(&self, job_id: u64, create_group: &CreateOneGroupJob) -> Result<()> {
        self.core
            .update(BackgroundJob {
                id: job_id,
                job: Some(background_job::Job::CreateOneGroup(create_group.to_owned())),
            })
            .await?;
        Ok(())
    }

    async fn handle_finish_create_group(
        &self,
        job: &BackgroundJob,
        create_group: CreateOneGroupJob,
    ) -> Result<()> {
        if matches!(
            CreateOneGroupStatus::from_i32(create_group.status).unwrap(),
            CreateOneGroupStatus::CreateOneGroupFinish
        ) {
            self.core
                .heartbeat_queue
                .try_schedule(
                    create_group
                        .invoked_nodes
                        .iter()
                        .cloned()
                        .map(|node_id| HeartbeatTask { node_id })
                        .collect(),
                    Instant::now(),
                )
                .await;
        }
        let mut job = job.to_owned();
        job.job = Some(background_job::Job::CreateOneGroup(create_group));
        self.core.finish(job).await?;
        Ok(())
    }

    fn record_create_group_step(step: &CreateOneGroupStatus) -> Option<HistogramTimer> {
        match step {
            CreateOneGroupStatus::CreateOneGroupInit => Some(
                metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS
                    .init
                    .start_timer(),
            ),
            CreateOneGroupStatus::CreateOneGroupCreating => Some(
                metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS
                    .create
                    .start_timer(),
            ),
            CreateOneGroupStatus::CreateOneGroupRollbacking => Some(
                metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS
                    .rollback
                    .start_timer(),
            ),
            CreateOneGroupStatus::CreateOneGroupFinish
            | CreateOneGroupStatus::CreateOneGroupAbort => Some(
                metrics::RECONCILE_CREATE_GROUP_STEP_DURATION_SECONDS
                    .finish
                    .start_timer(),
            ),
        }
    }
}

impl Jobs {
    async fn handle_purge_collection(
        &self,
        job: &BackgroundJob,
        purge_collection: &PurgeCollectionJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        let mut group_shards = schema
            .get_collection_shards(purge_collection.collection_id)
            .await?;
        loop {
            if let Some((group, shard)) = group_shards.pop() {
                self.try_remove_shard(group, shard.id).await?;
                continue;
            }
            break;
        }
        self.core.finish(job.to_owned()).await?;
        Ok(())
    }

    async fn handle_purge_database(
        &self,
        job: &BackgroundJob,
        purge_database: &PurgeDatabaseJob,
    ) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        let mut collections = schema
            .list_database_collections(purge_database.database_id)
            .await?;
        loop {
            if let Some(co) = collections.pop() {
                let job = BackgroundJob {
                    job: Some(Job::PurgeCollection(PurgeCollectionJob {
                        database_id: co.db,
                        collection_id: co.id,
                        database_name: "".to_owned(),
                        collection_name: co.name.to_owned(),
                        created_time: format!("{:?}", Instant::now()),
                    })),
                    ..Default::default()
                };
                match self.submit(job, false).await {
                    Ok(_) => {}
                    Err(crate::Error::AlreadyExists(_)) => {}
                    Err(err) => return Err(err),
                };
                schema.delete_collection(co).await?;
                continue;
            }
            break;
        }
        self.core.finish(job.to_owned()).await?;
        Ok(())
    }
}

impl Jobs {
    async fn try_create_shard(&self, group_id: u64, desc: &ShardDesc) -> Result<()> {
        let mut group_client = GroupClient::lazy(
            group_id,
            self.core.root_shared.provider.router.clone(),
            self.core.root_shared.provider.conn_manager.clone(),
        );
        group_client.create_shard(desc).await?;
        Ok(())
    }

    async fn try_create_replica(
        &self,
        addr: &str,
        replica_id: &u64,
        group: GroupDesc,
    ) -> Result<()> {
        let client = self
            .core
            .root_shared
            .provider
            .conn_manager
            .get_node_client(addr.to_owned())?;
        client.create_replica(replica_id.to_owned(), group).await?;
        Ok(())
    }

    async fn try_remove_replica(&self, group: u64, replica: u64) -> Result<()> {
        let schema = self.core.root_shared.schema()?;
        let rs = schema.get_replica_state(group, replica).await?.ok_or(
            crate::Error::AbortScheduleTask("source replica already has be destroyed"),
        )?;

        let target_node = schema
            .get_node(rs.node_id.to_owned())
            .await?
            .ok_or(crate::Error::AbortScheduleTask("source node not exist"))?;
        let client = self
            .core
            .root_shared
            .provider
            .conn_manager
            .get_node_client(target_node.addr.to_owned())?;
        client
            .remove_replica(
                replica.to_owned(),
                GroupDesc {
                    id: group,
                    ..Default::default()
                },
            )
            .await?;
        schema.remove_replica_state(group, replica).await?;
        Ok(())
    }

    async fn try_remove_shard(&self, _group: u64, _shard: u64) -> Result<()> {
        // TODO: impl remove shard.
        Ok(())
    }
}

struct JobCore {
    root_shared: Arc<RootShared>,
    mem_jobs: Arc<Mutex<MemJobs>>,
    res_locks: Arc<Mutex<HashSet<Vec<u8>>>>,
    alloc: Arc<Allocator<SysAllocSource>>,
    heartbeat_queue: Arc<HeartbeatQueue>,
    shard_balancer: Arc<ShardBalancer<SysAllocSource>>,
    enable: atomic::AtomicBool,
}

#[derive(Default)]
struct MemJobs {
    jobs: Vec<BackgroundJob>,
    removed_wakers: Vec<Waker>,
    added_wakers: Vec<Waker>,
}

impl JobCore {
    fn check_root_leader(&self) -> Result<()> {
        if !self.enable.load(atomic::Ordering::Relaxed) {
            return Err(crate::Error::NotRootLeader(RootDesc::default(), 0, None));
        }
        Ok(())
    }

    pub async fn recovery(&self) -> Result<()> {
        let schema = self.root_shared.schema()?;
        let jobs = schema.list_job().await?;
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            mem_jobs.jobs.clear();
            let wakers = std::mem::take(&mut mem_jobs.removed_wakers);
            for waker in wakers {
                waker.wake();
            }
            mem_jobs.jobs.extend_from_slice(&jobs);
            let wakers = std::mem::take(&mut mem_jobs.added_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        {
            let mut res_locks = self.res_locks.lock().unwrap();
            res_locks.clear();
            for job in &jobs {
                if let Some(key) = res_key(job) {
                    res_locks.insert(key);
                }
            }
        }
        Ok(())
    }

    pub fn on_drop_leader(&self) {
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            let wakers = std::mem::take(&mut mem_jobs.removed_wakers);
            for waker in wakers {
                waker.wake();
            }
            let wakers = std::mem::take(&mut mem_jobs.added_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        {
            let mut res_locks = self.res_locks.lock().unwrap();
            res_locks.clear();
        }
    }

    pub async fn append(&self, job: BackgroundJob) -> Result<BackgroundJob> {
        let schema = self.root_shared.schema()?;
        if let Some(res_key) = res_key(&job) {
            if !self.try_lock_res(res_key) {
                return Err(crate::Error::AlreadyExists(
                    "job for target resource already exist".into(),
                ));
            }
        }
        let job = schema.append_job(job).await?;
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            mem_jobs.jobs.push(job.to_owned());
            let wakers = std::mem::take(&mut mem_jobs.added_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        Ok(job)
    }

    pub async fn finish(&self, job: BackgroundJob) -> Result<()> {
        let schema = self.root_shared.schema()?;
        schema.remove_job(&job).await?;
        {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            mem_jobs.jobs.retain(|j| j.id != job.id);
            let wakers = std::mem::take(&mut mem_jobs.removed_wakers);
            for waker in wakers {
                waker.wake();
            }
        }
        if let Some(res_key) = res_key(&job) {
            self.unlock_res(&res_key);
        }
        Ok(())
    }

    pub async fn update(&self, job: BackgroundJob) -> Result<()> {
        let schema = self.root_shared.schema()?;
        let updated = schema.update_job(job.to_owned()).await?;
        if updated {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            if let Some(idx) = mem_jobs.jobs.iter().position(|j| j.id == job.id) {
                let _ = std::mem::replace(&mut mem_jobs.jobs[idx], job);
            }
        }
        Ok(())
    }

    pub async fn wait_more_jobs(&self) {
        poll_fn(|ctx| {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            if mem_jobs.jobs.is_empty() {
                mem_jobs.added_wakers.push(ctx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }

    pub async fn wait_and_check_result(&self, id: &u64) -> Result<()> {
        poll_fn(|ctx| {
            let mut mem_jobs = self.mem_jobs.lock().unwrap();
            if mem_jobs.jobs.iter().any(|j| j.id == *id) {
                mem_jobs.removed_wakers.push(ctx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;

        let job = self.get_history(id).await?;
        if job.is_none() {
            self.check_root_leader()?;
            unreachable!()
        }
        match job.unwrap().job.as_ref().unwrap() {
            background_job::Job::CreateCollection(job) => {
                match CreateCollectionJobStatus::from_i32(job.status).unwrap() {
                    CreateCollectionJobStatus::CreateCollectionFinish => Ok(()),
                    CreateCollectionJobStatus::CreateCollectionAbort => {
                        Err(crate::Error::InvalidArgument(format!(
                            "create collection fail {}",
                            job.remark
                        )))
                    }
                    _ => unreachable!(),
                }
            }
            background_job::Job::CreateOneGroup(job) => {
                match CreateOneGroupStatus::from_i32(job.status).unwrap() {
                    CreateOneGroupStatus::CreateOneGroupFinish => Ok(()),
                    CreateOneGroupStatus::CreateOneGroupAbort => {
                        Err(crate::Error::InvalidArgument("create group fail".into()))
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    pub async fn get_history(&self, id: &u64) -> Result<Option<BackgroundJob>> {
        let schema = self.root_shared.schema()?;
        schema.get_job_history(id).await
    }

    pub fn need_handle_jobs(&self) -> Vec<BackgroundJob> {
        let jobs = self.mem_jobs.lock().unwrap();
        jobs.jobs.to_owned()
    }

    fn try_lock_res(&self, res_key: Vec<u8>) -> bool {
        let mut res_locks = self.res_locks.lock().unwrap();
        res_locks.insert(res_key)
    }

    fn unlock_res(&self, res_key: &[u8]) {
        let mut res_locks = self.res_locks.lock().unwrap();
        res_locks.remove(res_key);
    }
}

fn res_key(job: &BackgroundJob) -> Option<Vec<u8>> {
    match job.job.as_ref().unwrap() {
        background_job::Job::CreateCollection(job) => {
            let mut key = job.database.to_le_bytes().to_vec();
            key.extend_from_slice(job.collection_name.as_bytes());
            Some(key)
        }
        background_job::Job::PurgeCollection(job) => {
            let mut key = job.database_id.to_le_bytes().to_vec();
            key.extend_from_slice(job.collection_name.as_bytes());
            Some(key)
        }
        background_job::Job::CreateOneGroup(_) | background_job::Job::PurgeDatabase(_) => None,
    }
}
