use std::{future::Future, task::Poll};
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
use std::{pin::Pin, task::Context};

#[allow(unused)]
#[derive(Debug)]
pub enum TaskPriority {
    Real,
    High,
    Middle,
    Low,
    IoLow,
    IoHigh,
}

/// A handle that awaits the result of a task.
///
/// Dropping a [`JoinHandle`] will detach the task, meaning that there is no longer
/// a handle to the task and no way to `join` on it.
#[derive(Debug)]
pub struct JoinHandle<T> {
    inner: tokio::task::JoinHandle<T>,
}

/// An execution service.
#[allow(unused)]
pub struct Executor
where
    Self: Send + Sync, {}

#[allow(unused)]
impl Executor {
    /// New executor and setup the underlying threads, scheduler.
    pub fn new(num_threads: usize) -> Self {
        todo!()
    }

    /// Spawns a task.
    ///
    /// [`tag`]: specify the tag of task, the underlying scheduler should ensure that all tasks
    ///          with the same tag will be scheduled on the same core.
    /// [`priority`]: specify the task priority.
    pub fn spawn<F, T>(tag: &[u8], priority: TaskPriority, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        todo!()
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
