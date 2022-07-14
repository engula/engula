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
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

pub struct ShutdownNotifier {
    core: Arc<Mutex<Core>>,
}

pub struct Shutdown {
    core: Arc<Mutex<Core>>,
}

struct Core {
    closed: bool,
    wakers: HashMap<usize, Waker>,
}

impl Shutdown {
    fn new(core: Arc<Mutex<Core>>) -> Shutdown {
        Shutdown { core }
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let shut = self.get_mut();
        let mut core = shut.core.lock().unwrap();
        if core.closed {
            Poll::Ready(())
        } else {
            let id = shut as *const Shutdown as usize;
            core.wakers.insert(id, cx.waker().clone());
            Poll::Pending
        }
    }
}

impl ShutdownNotifier {
    pub fn new() -> Self {
        ShutdownNotifier::default()
    }

    pub async fn ctrl_c(self) {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen ctrl c event");
    }

    pub fn subscribe(&self) -> Shutdown {
        Shutdown::new(self.core.clone())
    }
}

impl Default for ShutdownNotifier {
    fn default() -> Self {
        ShutdownNotifier {
            core: Arc::new(Mutex::new(Core {
                closed: false,
                wakers: HashMap::default(),
            })),
        }
    }
}

impl Drop for ShutdownNotifier {
    fn drop(&mut self) {
        let mut core = self.core.lock().unwrap();
        core.closed = true;
        for (_, waker) in std::mem::take(&mut core.wakers) {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::oneshot;

    use super::*;
    use crate::runtime::*;

    #[test]
    fn shutdown() {
        let notifier = ShutdownNotifier::new();
        let shutdown_1 = notifier.subscribe();
        let handle_1 = std::thread::spawn(|| {
            let owner = ExecutorOwner::new(1);
            owner.executor().block_on(async move {
                shutdown_1.await;
            });
        });

        let shutdown_2 = notifier.subscribe();
        let handle_2 = std::thread::spawn(|| {
            let owner = ExecutorOwner::new(1);
            owner.executor().block_on(async move {
                shutdown_2.await;
            });
        });

        drop(notifier);

        handle_1.join().unwrap_or_default();
        handle_2.join().unwrap_or_default();
    }

    #[test]
    fn shutdown_with_select() {
        let notifier = ShutdownNotifier::new();
        let (tx, rx) = oneshot::channel::<()>();
        let shutdown_1 = notifier.subscribe();
        let handle_1 = std::thread::spawn(|| {
            let owner = ExecutorOwner::new(1);
            owner.executor().block_on(async move {
                select! {
                    _ = rx => {}
                    _ = shutdown_1 => {}
                };
            });
        });

        drop(notifier);

        handle_1.join().unwrap_or_default();
        let _ = tx;
    }
}
