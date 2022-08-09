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
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::Future;
use tracing::debug;

use super::{
    event_source::EventSource,
    task::{Task, TaskState},
    tasks::GENERATED_TASK_ID,
};
use crate::{
    bootstrap::ROOT_GROUP_ID,
    node::{replica::ReplicaConfig, Replica},
    runtime::{sync::WaitGroup, TaskPriority},
    schedule::provider::GroupProviders,
    Provider,
};

#[derive(Clone)]
pub struct EventWaker {
    state: Arc<Mutex<WakerState>>,
}

#[derive(Default, Clone)]
struct EventWaiter {
    state: Arc<Mutex<WakerState>>,
}

#[derive(Default)]
struct WakerState {
    waked: bool,
    inner_waker: Option<std::task::Waker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Deadline {
    deadline: Instant,
    timer_id: u64,
    task_id: u64,
}

#[derive(Default)]
struct TaskTimer {
    next_timer_id: u64,
    timer_heap: BinaryHeap<Deadline>,
    timer_indexes: HashMap</* task_id */ u64, /* timer_id */ u64>,
}

pub struct ScheduleContext<'a> {
    pub group_id: u64,
    pub replica_id: u64,
    pub current_term: u64,
    pub execution_time: Duration,
    pub replica: Arc<Replica>,
    pub(crate) provider: Arc<Provider>,
    pub cfg: &'a ReplicaConfig,
    next_task_id: &'a mut u64,
    pending_tasks: &'a mut Vec<Box<dyn Task>>,
}

pub struct Scheduler
where
    Self: Send,
{
    group_id: u64,
    replica_id: u64,
    cfg: ReplicaConfig,
    replica: Arc<Replica>,
    provider: Arc<Provider>,

    event_waiter: EventWaiter,
    event_sources: Vec<Arc<dyn EventSource>>,
    next_task_id: u64,
    jobs: HashMap<u64, Job>,
    timer: TaskTimer,
}

struct Job {
    _start_at: Instant,
    advanced_at: Instant,

    task: Box<dyn Task>,
}

impl Scheduler {
    fn new(
        cfg: ReplicaConfig,
        replica: Arc<Replica>,
        provider: Arc<Provider>,
        event_sources: Vec<Arc<dyn EventSource>>,
    ) -> Self {
        let info = replica.replica_info();
        let group_id = info.group_id;
        let replica_id = info.replica_id;
        let event_waiter = EventWaiter::new();
        for source in &event_sources {
            source.bind(event_waiter.waker());
        }
        Scheduler {
            group_id,
            replica_id,
            replica,
            provider,
            cfg,

            event_sources,
            event_waiter,
            next_task_id: GENERATED_TASK_ID,
            jobs: HashMap::default(),
            timer: TaskTimer::new(),
        }
    }

    #[inline]
    async fn wait_new_events(&mut self) {
        self.timer.timeout(self.event_waiter.clone()).await;
    }

    async fn advance(&mut self, current_term: u64, mut pending_tasks: Vec<Box<dyn Task>>) {
        let mut active_tasks = self.collect_active_tasks();
        while !active_tasks.is_empty() {
            for task_id in active_tasks {
                self.advance_task(current_term, task_id, &mut pending_tasks)
                    .await;
                crate::runtime::yield_now().await;
            }

            // Poll the new tasks immediately.
            active_tasks = pending_tasks.iter().map(|t| t.id()).collect();
            self.insert_tasks(std::mem::take(&mut pending_tasks));
        }
    }

    async fn advance_task(
        &mut self,
        current_term: u64,
        task_id: u64,
        pending_tasks: &mut Vec<Box<dyn Task>>,
    ) {
        if let Some(job) = self.jobs.get_mut(&task_id) {
            loop {
                let mut ctx = ScheduleContext {
                    cfg: &self.cfg,
                    group_id: self.group_id,
                    replica_id: self.replica_id,
                    replica: self.replica.clone(),
                    provider: self.provider.clone(),
                    current_term,
                    execution_time: Instant::now().duration_since(job.advanced_at),
                    next_task_id: &mut self.next_task_id,
                    pending_tasks,
                };
                match job.task.poll(&mut ctx).await {
                    TaskState::Pending(interval) => {
                        if let Some(interval) = interval {
                            self.timer.add(task_id, Instant::now() + interval);
                        }
                        break;
                    }
                    TaskState::Advanced => {
                        job.advanced_at = Instant::now();
                    }
                    TaskState::Terminated => {
                        self.jobs.remove(&task_id);
                        return;
                    }
                }
            }
        }
    }

    fn insert_tasks(&mut self, tasks: Vec<Box<dyn Task>>) {
        let now = Instant::now();
        for task in tasks {
            let task_id = task.id();
            self.jobs.insert(
                task_id,
                Job {
                    _start_at: now,
                    advanced_at: now,
                    task,
                },
            );
        }
    }

    fn collect_active_tasks(&mut self) -> HashSet<u64> {
        let mut active_tasks = self.timer.take_fired_tasks();
        for source in &mut self.event_sources {
            active_tasks.extend(source.active_tasks().iter());
        }
        active_tasks
    }
}

impl<'a> ScheduleContext<'a> {
    #[inline]
    pub fn delegate(&mut self, task: Box<dyn Task>) {
        self.pending_tasks.push(task);
    }

    #[inline]
    pub fn next_task_id(&mut self) -> u64 {
        let task_id = *self.next_task_id;
        *self.next_task_id += 1;
        task_id
    }
}

impl EventWaker {
    pub fn wake(&self) {
        let mut state = self.state.lock().expect("poisoned");
        state.waked = true;
        if let Some(waker) = state.inner_waker.take() {
            waker.wake();
        }
    }
}

impl EventWaiter {
    fn new() -> Self {
        EventWaiter::default()
    }

    fn waker(&self) -> EventWaker {
        EventWaker {
            state: self.state.clone(),
        }
    }
}

impl std::future::Future for EventWaiter {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.state.lock().expect("poisoned");
        if state.waked {
            state.waked = false;
            state.inner_waker = None;
            Poll::Ready(())
        } else {
            state.inner_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.timer_id.cmp(&self.timer_id))
    }
}

impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TaskTimer {
    fn new() -> Self {
        TaskTimer::default()
    }

    fn add(&mut self, task_id: u64, deadline: Instant) {
        let timer_id = self.next_timer_id;
        self.next_timer_id += 1;
        self.timer_indexes.insert(task_id, timer_id);
        self.timer_heap.push(Deadline {
            deadline,
            timer_id,
            task_id,
        });
    }

    async fn timeout<T: Future<Output = ()>>(&self, f: T) {
        if let Some(event) = self.timer_heap.peek() {
            let _ = tokio::time::timeout_at(event.deadline.into(), f).await;
        } else {
            f.await;
        }
    }

    fn take_fired_tasks(&mut self) -> HashSet<u64> {
        let now = Instant::now();
        let mut result = HashSet::default();
        while let Some(Deadline {
            deadline,
            timer_id,
            task_id,
        }) = self.timer_heap.peek()
        {
            if *deadline > now {
                break;
            }
            if self
                .timer_indexes
                .get(task_id)
                .map(|t| *t == *timer_id)
                .unwrap_or_default()
            {
                self.timer_indexes.remove(task_id);
                result.insert(*task_id);
            }
            self.timer_heap.pop();
        }
        result
    }
}

pub(crate) fn setup_scheduler(
    cfg: ReplicaConfig,
    provider: Arc<Provider>,
    replica: Arc<Replica>,
    wait_group: WaitGroup,
) {
    let group_providers = Arc::new(GroupProviders::new(
        replica.clone(),
        provider.router.clone(),
    ));

    let group_id = replica.replica_info().group_id;
    let tag = &group_id.to_le_bytes();
    let executor = provider.executor.clone();
    executor.spawn(Some(tag), TaskPriority::Low, async move {
        scheduler_main(cfg, replica, provider, group_providers).await;
        drop(wait_group);
    });
}

async fn scheduler_main(
    cfg: ReplicaConfig,
    replica: Arc<Replica>,
    provider: Arc<Provider>,
    group_providers: Arc<GroupProviders>,
) {
    let info = replica.replica_info();
    let group_id = info.group_id;
    let replica_id = info.replica_id;
    drop(info);

    while let Ok(Some(current_term)) = replica.on_leader("scheduler", false).await {
        let providers: Vec<Arc<dyn EventSource>> = vec![
            group_providers.descriptor.clone(),
            group_providers.replica_states.clone(),
            group_providers.raft_state.clone(),
        ];
        let mut scheduler =
            Scheduler::new(cfg.clone(), replica.clone(), provider.clone(), providers);
        let mut pending_tasks = vec![];
        allocate_group_tasks(&mut pending_tasks, group_providers.clone()).await;
        if group_id == ROOT_GROUP_ID {
            setup_root_tasks(&mut pending_tasks).await;
        }
        scheduler.advance(current_term, pending_tasks).await;
        while let Ok(Some(term)) = replica.on_leader("scheduler", true).await {
            if term != current_term {
                break;
            }
            scheduler.wait_new_events().await;
            scheduler.advance(current_term, vec![]).await;
        }
    }
    debug!("group {group_id} replica {replica_id} scheduler is stopped");
}

async fn allocate_group_tasks(
    pending_tasks: &mut Vec<Box<dyn Task>>,
    providers: Arc<GroupProviders>,
) {
    use super::tasks::*;

    pending_tasks.push(Box::new(WatchReplicaStates::new(providers.clone())));
    pending_tasks.push(Box::new(WatchRaftState::new(providers.clone())));
    pending_tasks.push(Box::new(WatchGroupDescriptor::new(providers.clone())));
    pending_tasks.push(Box::new(PromoteGroup::new(providers.clone())));
    pending_tasks.push(Box::new(CureGroup::new(providers.clone())));
    pending_tasks.push(Box::new(RemoveOrphanReplica::new(providers)));
}

#[allow(clippy::ptr_arg)]
async fn setup_root_tasks(_pending_tasks: &mut Vec<Box<dyn Task>>) {
    // TODO(walter) add root related scheduler tasks.
}
