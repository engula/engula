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
mod metrics;
mod shutdown;
pub mod sync;
pub mod time;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use serde::{Deserialize, Serialize};
pub use tokio::select;

use self::metrics::*;
pub use self::shutdown::{Shutdown, ShutdownNotifier};

#[derive(Debug)]
pub enum TaskPriority {
    Real,
    High,
    Middle,
    Low,
    IoLow,
    IoHigh,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub event_interval: Option<u32>,
    pub global_event_interval: Option<u32>,
}

/// A handle that awaits the result of a task.
///
/// Dropping a [`JoinHandle`] will detach the task, meaning that there is no longer
/// a handle to the task and no way to `join` on it.
#[derive(Debug)]
pub struct JoinHandle<T> {
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
        tag: Option<&[u8]>,
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
        let inner = self.handle.spawn(future);
        JoinHandle { inner }
    }

    /// Runs a future to completion on the executor. This is the executor’s entry point.
    pub fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send + 'static,
    {
        self.handle.block_on(future)
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

pub async fn yield_now() {
    tokio::task::yield_now().await;
}
