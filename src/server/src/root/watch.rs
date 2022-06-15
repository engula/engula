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
    sync::Arc,
    task::{Poll, Waker},
};

use engula_api::server::v1::{
    watch_response::{DeleteEvent, UpdateEvent},
    WatchResponse,
};
use futures::Stream;
use tokio::sync::{RwLock, RwLockWriteGuard};

#[derive(Default)]
pub struct WatchHub {
    inner: Arc<RwLock<WatchHubInner>>,
}

#[derive(Default)]
pub struct WatchHubInner {
    next_watcher_id: u64,
    watchers: HashMap<u64, Watcher>,
}

pub type WriteGuard<'a> = RwLockWriteGuard<'a, WatchHubInner>;

impl WatchHub {
    pub async fn create_watcher(&self) -> (Watcher, WriteGuard) {
        let mut inner = self.inner.write().await;
        inner.next_watcher_id += 1;
        let watcher = Watcher {
            id: inner.next_watcher_id,
            init_resp: None,
            inner: Default::default(),
        };
        (watcher, inner)
    }

    pub async fn remove_watcher(&self, id: u64) {
        let mut inner = self.inner.write().await;
        inner.watchers.remove(&id);
    }

    pub async fn notify(&self, updates: Vec<UpdateEvent>, deletes: Vec<DeleteEvent>) {
        let inner = self.inner.read().await;
        for w in inner.watchers.values() {
            w.notify(&updates, &deletes)
        }
    }

    pub async fn cleanup(&self) {
        let mut inner = self.inner.write().await;
        inner
            .watchers
            .retain(|_, w| !w.inner.lock().unwrap().dropped);
    }
}

pub struct Watcher {
    #[allow(dead_code)]
    id: u64,
    init_resp: Option<WatchResponse>,
    inner: Arc<std::sync::Mutex<WatcherInner>>,
}

#[derive(Default)]
struct WatcherInner {
    waker: Option<Waker>,
    updates: Vec<UpdateEvent>,
    deletes: Vec<DeleteEvent>,
    dropped: bool,
}

impl Watcher {
    pub fn set_init_resp(&mut self, updates: Vec<UpdateEvent>, deletes: Vec<DeleteEvent>) {
        self.init_resp = Some(WatchResponse { updates, deletes })
    }

    fn notify(&self, updates: &[UpdateEvent], deletes: &[DeleteEvent]) {
        let mut inner = self.inner.lock().unwrap();
        if inner.dropped {
            return;
        }
        inner.updates.extend_from_slice(updates); // TODO: set capcity limit
        inner.deletes.extend_from_slice(deletes);
        if let Some(w) = inner.waker.take() {
            w.wake();
        }
    }
}

impl Stream for Watcher {
    type Item = std::result::Result<WatchResponse, tonic::Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(init_resp) = self.init_resp.take() {
            return Poll::Ready(Some(Ok(init_resp)));
        }
        let mut inner = self.inner.lock().unwrap();
        if !inner.updates.is_empty() || !inner.deletes.is_empty() {
            let resp = WatchResponse {
                updates: std::mem::take(&mut inner.updates),
                deletes: std::mem::take(&mut inner.deletes),
            };
            return Poll::Ready(Some(Ok(resp)));
        }
        if inner.waker.is_none() {
            inner.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.dropped = true;
    }
}
