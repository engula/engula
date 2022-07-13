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
};

use futures::pin_mut;
use tokio::sync::broadcast;

pub struct ShutdownNotifier {
    sender: broadcast::Sender<()>,
}

pub struct Shutdown {
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown { notify }
    }

    pub async fn recv(&mut self) {
        let _ = self.notify.recv().await;
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = self.get_mut().recv();
        pin_mut!(fut);
        fut.poll(cx)
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
        Shutdown::new(self.sender.subscribe())
    }
}

impl Default for ShutdownNotifier {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(1);
        ShutdownNotifier { sender: tx }
    }
}
