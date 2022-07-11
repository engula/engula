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
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

#[derive(Default)]
pub struct WaitGroup {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    wakers: Vec<Waker>,
    count: usize,
}

impl WaitGroup {
    pub fn new() -> Self {
        WaitGroup::default()
    }

    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().count
    }

    pub async fn wait(self) {
        use futures::future::poll_fn;

        if self.inner.lock().unwrap().count == 1 {
            return;
        }

        let inner = self.inner.clone();
        drop(self);

        poll_fn(|ctx| {
            let mut inner = inner.lock().unwrap();
            if inner.count == 0 {
                Poll::Ready(())
            } else {
                inner.wakers.push(ctx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
}

impl Clone for WaitGroup {
    fn clone(&self) -> Self {
        let mut inner = self.inner.lock().unwrap();
        inner.count += 1;

        WaitGroup {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for WaitGroup {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.count -= 1;

        if inner.count == 0 {
            for w in std::mem::take(&mut inner.wakers) {
                w.wake();
            }
        }
    }
}

impl Default for Inner {
    fn default() -> Self {
        Inner {
            wakers: vec![],
            count: 1,
        }
    }
}
