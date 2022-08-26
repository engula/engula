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
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    vec,
};

use engula_api::server::v1::{
    watch_response::{DeleteEvent, UpdateEvent},
    WatchResponse,
};
use futures::Stream;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::Error;

#[derive(Default)]
pub struct WatchHub {
    inner: Arc<RwLock<WatchHubInner>>,
}

#[derive(Default)]
pub struct WatchHubInner {
    next_watcher_id: u64,
    watchers: HashMap<u64, Watcher>,
}

pub struct WatcherInitializer<'a> {
    _guard: RwLockWriteGuard<'a, WatchHubInner>,
    watcher_inner: Arc<Mutex<WatcherInner>>,
}

impl<'a> WatcherInitializer<'a> {
    pub fn set_init_resp(&mut self, updates: Vec<UpdateEvent>, deletes: Vec<DeleteEvent>) {
        let mut inner = self.watcher_inner.lock().unwrap();
        inner.updates.extend_from_slice(&updates);
        inner.deletes.extend_from_slice(&deletes);
    }
}

impl WatchHub {
    pub async fn create_watcher(&self) -> (Watcher, WatcherInitializer) {
        let mut inner = self.inner.write().await;
        inner.next_watcher_id += 1;
        let watcher_inner = Arc::new(Mutex::new(WatcherInner::default()));
        let watcher = Watcher {
            id: inner.next_watcher_id,
            inner: watcher_inner.to_owned(),
        };
        inner.watchers.insert(watcher.id, watcher.to_owned());
        super::metrics::WATCH_TABLE_SIZE.set(inner.watchers.len() as i64);
        (
            watcher,
            WatcherInitializer {
                _guard: inner,
                watcher_inner,
            },
        )
    }

    pub async fn remove_watcher(&self, id: u64) {
        let mut inner = self.inner.write().await;
        inner.watchers.remove(&id);
        super::metrics::WATCH_TABLE_SIZE.set(inner.watchers.len() as i64);
    }

    pub async fn notify_updates(&self, updates: Vec<UpdateEvent>) {
        self.notify(updates, vec![], None).await;
    }

    pub async fn notify_deletes(&self, deletes: Vec<DeleteEvent>) {
        self.notify(vec![], deletes, None).await;
    }

    pub async fn notify_error(&self, err: Error) {
        self.notify(vec![], vec![], Some(err)).await;
    }

    async fn notify(
        &self,
        updates: Vec<UpdateEvent>,
        deletes: Vec<DeleteEvent>,
        _err: Option<Error>,
    ) {
        let inner = self.inner.read().await;
        for w in inner.watchers.values() {
            w.notify(&updates, &deletes, None) // TODO: clonable error
        }
    }

    pub async fn cleanup(&self) {
        let mut inner = self.inner.write().await;
        inner
            .watchers
            .retain(|_, w| !w.inner.lock().unwrap().dropped);
        super::metrics::WATCH_TABLE_SIZE.set(inner.watchers.len() as i64);
    }
}

#[derive(Clone)]
pub struct Watcher {
    #[allow(dead_code)]
    id: u64,
    inner: Arc<std::sync::Mutex<WatcherInner>>,
}

#[derive(Default)]
struct WatcherInner {
    waker: Option<Waker>,
    updates: Vec<UpdateEvent>,
    deletes: Vec<DeleteEvent>,
    err: Option<Error>,
    dropped: bool,
}

impl Watcher {
    fn notify(&self, updates: &[UpdateEvent], deletes: &[DeleteEvent], err: Option<Error>) {
        let _timer = super::metrics::WATCH_NOTIFY_DURATION_SECONDS.start_timer();
        let mut inner = self.inner.lock().unwrap();
        if inner.dropped {
            return;
        }
        inner.updates.extend_from_slice(updates); // TODO: set capcity limit
        inner.deletes.extend_from_slice(deletes);
        if err.is_some() && inner.err.is_none() {
            inner.err = err
        }
        if let Some(w) = inner.waker.take() {
            w.wake();
        }
    }
}

impl Stream for Watcher {
    type Item = std::result::Result<WatchResponse, tonic::Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.dropped {
            return Poll::Ready(None);
        }
        if let Some(err) = inner.err.take() {
            return Poll::Ready(Some(Err(err.into())));
        }
        if !inner.updates.is_empty() || !inner.deletes.is_empty() {
            let resp = WatchResponse {
                updates: std::mem::take(&mut inner.updates),
                deletes: std::mem::take(&mut inner.deletes),
            };
            return Poll::Ready(Some(Ok(resp)));
        }
        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.dropped = true;
    }
}
