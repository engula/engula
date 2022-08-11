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

use engula_api::server::v1::{RootDesc, ShardDesc};
use engula_client::GroupClient;
use futures::future::poll_fn;
use tracing::error;

use super::{allocator::*, RootShared};
use crate::{serverpb::v1::*, Result};

pub struct Jobs {
    core: JobCore,
}

impl Jobs {
    pub fn new(root_shared: Arc<RootShared>, alloc: Arc<Allocator<SysAllocSource>>) -> Self {
        Self {
            core: JobCore {
                root_shared,
                alloc,
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
        match job.job.as_ref().unwrap() {
            background_job::Job::CreateCollection(create_collection) => {
                self.handle_create_collection(job, create_collection).await
            }
        }
    }
}

impl Jobs {
    async fn handle_create_collection(
        &self,
        job: &BackgroundJob,
        create_collection: &CreateCollectionJob,
    ) -> Result<()> {
        let mut create_collection = create_collection.to_owned();
        loop {
            match CreateCollectionJobStatus::from_i32(create_collection.status).unwrap() {
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
            let groups = self.core.alloc.place_group_for_shard(1).await?;
            if groups.is_empty() {
                return Err(crate::Error::ResourceExhausted("no engouth groups".into()));
            }
            let group = groups.first().unwrap();
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
}

impl Jobs {
    async fn try_create_shard(&self, group_id: u64, desc: &ShardDesc) -> Result<()> {
        let mut group_client = GroupClient::new(
            group_id,
            self.core.root_shared.provider.router.clone(),
            self.core.root_shared.provider.conn_manager.clone(),
        );
        group_client.create_shard(desc).await?;
        Ok(())
    }
}

struct JobCore {
    root_shared: Arc<RootShared>,
    mem_jobs: Arc<Mutex<MemJobs>>,
    res_locks: Arc<Mutex<HashSet<Vec<u8>>>>,
    alloc: Arc<Allocator<SysAllocSource>>,
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
                res_locks.insert(res_key(job));
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
        let res_key = res_key(&job);
        if !self.try_lock_res(res_key) {
            return Err(crate::Error::AlreadyExists(
                "job for target resource already exist".into(),
            ));
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
        let res_key = res_key(&job);
        self.unlock_res(&res_key);
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

fn res_key(job: &BackgroundJob) -> Vec<u8> {
    match job.job.as_ref().unwrap() {
        background_job::Job::CreateCollection(job) => {
            let mut key = job.database.to_le_bytes().to_vec();
            key.extend_from_slice(job.collection_name.as_bytes());
            key
        }
    }
}
