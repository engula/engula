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
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::{Stream, TryStreamExt};
use log::warn;
use tokio::task::JoinHandle;

use crate::{
    policy::{GroupReader, Policy as ReplicatePolicy},
    store::{Transport, TryBatchNext},
    Entry, Result,
};

/// EventGroupReader likes `GroupReader`, but returns a sequence of events
/// instead of entries.
struct EventGroupReader {
    reader: GroupReader,
}

impl EventGroupReader {
    #[inline(always)]
    fn append(&mut self, target: &str, entries: Vec<(u32, Entry)>) {
        self.reader.append(target, entries);
    }

    #[inline(always)]
    fn target_next_index(&self, target: &str) -> u32 {
        self.reader.target_next_index(target)
    }

    fn take_next_entry(&mut self) -> Poll<Option<Box<[u8]>>> {
        while let Some((_, e)) = self.reader.take_next_entry() {
            let ready = match e {
                Entry::Event { event, .. } => Some(event),
                Entry::Bridge { .. } => None,
                Entry::Hole => {
                    continue;
                }
            };
            return Poll::Ready(ready);
        }

        Poll::Pending
    }
}

struct ReaderCore {
    waker: Option<Waker>,
    group_reader: EventGroupReader,
}

impl ReaderCore {
    #[inline]
    fn append(&mut self, target: &str, entries: Vec<(u32, Entry)>) {
        self.group_reader.append(target, entries);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    #[inline(always)]
    fn target_start_index(&self, target: &str) -> u32 {
        self.group_reader.target_next_index(target)
    }
}

struct ReaderHandle {
    transport: Transport,
    core: Mutex<ReaderCore>,
}

impl ReaderHandle {
    #[inline(always)]
    async fn append(&self, target: &str, entries: Vec<(u32, Entry)>) {
        let mut core = self.core.lock().unwrap();
        core.append(target, entries);
    }

    #[inline(always)]
    async fn target_start_index(&self, target: &str) -> u32 {
        let core = self.core.lock().unwrap();
        core.target_start_index(target)
    }
}

async fn read_replica_as_possible(
    handle: Arc<ReaderHandle>,
    target: String,
    stream_id: u64,
    segment_epoch: u32,
) -> Result<()> {
    let start_index = handle.target_start_index(&target).await;
    let mut streaming = handle
        .transport
        .read(target.clone(), stream_id, segment_epoch, start_index, true)
        .await?;

    let mut streaming = TryBatchNext::new(&mut streaming);
    while let Some(entries) = streaming.try_next().await? {
        handle.append(&target, entries).await;
    }

    handle.append(&target, vec![]).await;
    Ok(())
}

async fn read_replica(
    handle: Arc<ReaderHandle>,
    target: String,
    stream_id: u64,
    segment_epoch: u32,
) {
    // TODO(w41ter) support backoff and error handling.
    while let Err(error) =
        read_replica_as_possible(handle.clone(), target.clone(), stream_id, segment_epoch).await
    {
        warn!("stream {} read entries: {}", stream_id, error);
    }
}

pub struct SegmentReader {
    handle: Arc<ReaderHandle>,
    replica_handles: Vec<JoinHandle<()>>,
}

impl SegmentReader {
    pub async fn new(
        replicate_policy: ReplicatePolicy,
        transport: Transport,
        stream_id: u64,
        segment_epoch: u32,
        start_index: Option<u32>,
        copy_set: Vec<String>,
    ) -> Self {
        let next_index = start_index.unwrap_or(1);
        let group_reader =
            replicate_policy.new_group_reader(segment_epoch, next_index, copy_set.clone());
        let group_reader = EventGroupReader {
            reader: group_reader,
        };

        let handle = Arc::new(ReaderHandle {
            transport,
            core: Mutex::new(ReaderCore {
                waker: None,
                group_reader,
            }),
        });
        let replica_handles = copy_set
            .into_iter()
            .map(|target| {
                let cloned_handle = handle.clone();
                tokio::spawn(async move {
                    read_replica(cloned_handle, target, stream_id, segment_epoch).await;
                })
            })
            .collect();
        SegmentReader {
            handle,
            replica_handles,
        }
    }
}

impl Stream for SegmentReader {
    type Item = Box<[u8]>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut core = this.handle.core.lock().unwrap();
        match core.group_reader.take_next_entry() {
            Poll::Pending => {
                core.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Poll::Ready(ready) => Poll::Ready(ready),
        }
    }
}

impl Drop for SegmentReader {
    fn drop(&mut self) {
        self.replica_handles.iter().for_each(|h| h.abort());
    }
}
