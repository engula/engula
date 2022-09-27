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
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use pin_project::pin_project;
use serde::{Deserialize, Serialize};

use super::metrics::*;

#[derive(Debug)]
pub enum TaskPriority {
    Real,
    High,
    Middle,
    Low,
    IoLow,
    IoHigh,
}

enum TaskState {
    First(Instant),
    Polled(Duration),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub event_interval: Option<u32>,
    pub global_event_interval: Option<u32>,
    pub max_blocking_threads: Option<usize>,
}

/// A handle that awaits the result of a task.
///
/// Dropping a [`JoinHandle`] will detach the task, meaning that there is no longer
/// a handle to the task and no way to `join` on it. If you want cancel tasks when
/// dropping a handle, use [`DispatchHandle`].
#[derive(Debug)]
pub struct JoinHandle<T> {
    inner: tokio::task::JoinHandle<T>,
}

/// A handle that awaits the result of a task.
///
/// Dropping a [`DispatchHandle`] will abort the underlying task.
#[derive(Debug)]
pub struct DispatchHandle<T> {
    inner: tokio::task::JoinHandle<T>,
}

pub struct ExecutorOwner {
    runtime: tokio::runtime::Runtime,
}

/// An execution service.
#[derive(Clone)]
pub struct Executor
where
    Self: Send + Sync,
{
    handle: tokio::runtime::Handle,
}

#[pin_project]
struct FutureWrapper<F: Future> {
    #[pin]
    inner: F,
    state: TaskState,
}

impl ExecutorOwner {
    /// New executor and setup the underlying threads, scheduler.
    pub fn new(num_threads: usize) -> Self {
        Self::with_config(num_threads, ExecutorConfig::default())
    }

    pub fn with_config(num_threads: usize, cfg: ExecutorConfig) -> Self {
        use tokio::runtime::Builder;
        let runtime = Builder::new_multi_thread()
            .worker_threads(num_threads)
            .enable_all()
            .event_interval(cfg.event_interval.unwrap_or(61))
            .global_queue_interval(cfg.global_event_interval.unwrap_or(64))
            .max_blocking_threads(cfg.max_blocking_threads.unwrap_or(2))
            .thread_keep_alive(Duration::from_secs(60))
            .on_thread_park(|| {
                EXECUTOR_PARK_TOTAL.inc();
            })
            .on_thread_unpark(|| {
                EXECUTOR_UNPARK_TOTAL.inc();
            })
            .build()
            .expect("build tokio runtime");
        ExecutorOwner { runtime }
    }

    pub fn executor(&self) -> Executor {
        Executor {
            handle: self.runtime.handle().clone(),
        }
    }
}

impl Executor {
    /// Spawns a task.
    ///
    /// [`tag`]: specify the tag of task, the underlying scheduler should ensure that all tasks
    ///          with the same tag will be scheduled on the same core.
    /// [`priority`]: specify the task priority.
    pub fn spawn<F, T>(
        &self,
        tag: Option<u64>,
        priority: TaskPriority,
        future: F,
    ) -> JoinHandle<F::Output>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // TODO(walter) support per thread task set.
        let _ = tag;
        take_spawn_metrics(priority);
        let inner = self.handle.spawn(FutureWrapper::new(future));
        JoinHandle { inner }
    }

    /// Dispatch a task.
    ///
    /// [`tag`]: specify the tag of task, the underlying scheduler should ensure that all tasks
    ///          with the same tag will be scheduled on the same core.
    /// [`priority`]: specify the task priority.
    pub fn dispatch<F, T>(
        &self,
        tag: Option<u64>,
        priority: TaskPriority,
        future: F,
    ) -> DispatchHandle<F::Output>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // TODO(walter) support per thread task set.
        let _ = tag;
        take_spawn_metrics(priority);
        let inner = self.handle.spawn(FutureWrapper::new(future));
        DispatchHandle { inner }
    }

    /// Runs a future to completion on the executor. This is the executor’s entry point.
    #[inline]
    pub fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send + 'static,
    {
        self.handle.block_on(future)
    }

    #[inline]
    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let inner = self.handle.spawn_blocking(func);
        JoinHandle { inner }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(v)) => Poll::Ready(v),
            Poll::Ready(Err(e)) => panic!("{:?}", e),
        }
    }
}

impl<T> Future for DispatchHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(v)) => Poll::Ready(v),
            Poll::Ready(Err(e)) => panic!("{:?}", e),
        }
    }
}

impl<T> Drop for DispatchHandle<T> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

impl<F: Future> FutureWrapper<F> {
    fn new(inner: F) -> Self {
        FutureWrapper {
            state: TaskState::First(Instant::now()),
            inner,
        }
    }
}

impl<F: Future> Future for FutureWrapper<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let mut duration = match this.state {
            TaskState::First(create) => {
                EXECUTOR_TASK_FIRST_POLL_DURATION_SECONDS.observe(create.elapsed().as_secs_f64());
                Duration::ZERO
            }
            TaskState::Polled(duration) => *duration,
        };

        let start = Instant::now();
        let output = Pin::new(&mut this.inner).poll(cx);
        let elapsed = start.elapsed();
        EXECUTOR_TASK_POLL_DURATION_SECONDS.observe(elapsed.as_secs_f64());
        if !should_skip_slow_log::<F>() && elapsed >= Duration::from_micros(1000) {
            tracing::warn!(
                "future poll() execute total {elapsed:?}: {}",
                std::any::type_name::<F>(),
            );
        }

        duration += elapsed;
        *this.state = TaskState::Polled(duration);
        if !matches!(&output, Poll::Pending) {
            EXECUTOR_TASK_EXECUTE_DURATION_SECONDS.observe(duration.as_secs_f64());
        }

        output
    }
}

/// Returns a `Executor` view over the currently running `ExecutorOwner`.
///
/// # Panics
///
/// This will panic if called outside the context of a runtime.
pub fn current() -> Executor {
    Executor {
        handle: tokio::runtime::Handle::current(),
    }
}

#[inline]
const fn should_skip_slow_log<F: Future>() -> bool {
    const_str::contains!(std::any::type_name::<F>(), "start_raft_group")
}
