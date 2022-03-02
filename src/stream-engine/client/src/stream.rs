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
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream as FutureStream;
use stream_engine_proto::*;
use tokio::sync::{Mutex, Notify};

use crate::{master::Stream as MasterStreamClient, Error, Result};

/// The role of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// A leader manipulate a stream.
    Leader,
    /// A follower subscribes a stream.
    Follower,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Role::Leader => "LEADER",
                Role::Follower => "FOLLOWER",
            }
        )
    }
}

impl From<i32> for Role {
    fn from(role: i32) -> Self {
        match stream_engine_proto::Role::from_i32(role) {
            Some(stream_engine_proto::Role::Follower) | None => Role::Follower,
            Some(stream_engine_proto::Role::Leader) => Role::Leader,
        }
    }
}

/// The role and leader's address of current epoch.
#[derive(Debug, Clone)]
pub struct EpochState {
    pub epoch: u64,

    /// The role of the associated stream.
    pub role: Role,

    /// The leader of the associated stream.
    pub leader: Option<String>,
}

#[derive(Clone)]
pub struct Stream {
    inner: Arc<StreamInner>,
}

impl Stream {
    pub(crate) fn new(stream_client: MasterStreamClient) -> Self {
        let inner = StreamInner {
            stream_client,
            core: Mutex::new(StreamCore::new()),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    #[inline(always)]
    pub fn desc(&self) -> StreamDesc {
        self.inner.stream_client.desc()
    }

    /// Return a endless stream which returns a new epoch state once the
    /// associated stream enters a new epoch.
    pub async fn subscribe_state(&self) -> Result<EpochStateStream> {
        Ok(EpochStateStream::new())
    }

    /// Returns a stream reader.
    pub async fn new_reader(&self) -> Result<StreamReader> {
        Ok(StreamReader::new(self.clone()))
    }

    /// Append an event, returns the sequence.
    pub async fn append(&self, event: Box<[u8]>) -> Result<u64> {
        let mut stream = self.inner.core.lock().await;
        stream.events.push_back(event);
        stream.end += 1;
        stream.waiter.notify_waiters();
        Ok(stream.end - 1)
    }

    /// Truncates events up to a sequence (exclusive).
    pub async fn truncate(&self, sequence: u64) -> Result<()> {
        let mut stream = self.inner.core.lock().await;
        if stream.end.checked_sub(sequence).is_some() {
            let offset = sequence.saturating_sub(stream.start);
            stream.events.drain(..offset as usize);
            stream.start += offset;
            Ok(())
        } else {
            Err(Error::InvalidArgument(format!(
                "truncate sequence (is {}) should be <= end (is {})",
                sequence, stream.end
            )))
        }
    }
}

struct StreamInner {
    stream_client: MasterStreamClient,
    core: Mutex<StreamCore>,
}

struct StreamCore {
    start: u64,
    end: u64,
    events: VecDeque<Box<[u8]>>,
    waiter: Arc<Notify>,
}

impl StreamCore {
    fn new() -> Self {
        StreamCore {
            start: 0,
            end: 0,
            events: VecDeque::new(),
            waiter: Arc::new(Notify::new()),
        }
    }
}

impl StreamCore {
    fn read_all(&self, from: u64) -> VecDeque<Box<[u8]>> {
        if self.end.checked_sub(from).is_some() {
            self.events
                .range(from.saturating_sub(self.start) as usize..)
                .cloned()
                .collect()
        } else {
            VecDeque::default()
        }
    }
}

pub struct StreamReader {
    cursor: u64,
    stream: Stream,
    events: VecDeque<Box<[u8]>>,
}

impl StreamReader {
    fn new(stream: Stream) -> Self {
        Self {
            cursor: 0,
            stream,
            events: VecDeque::new(),
        }
    }

    async fn next(&mut self, wait: bool) -> Result<Option<Box<[u8]>>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }

        let stream = self.stream.inner.core.lock().await;
        let events = stream.read_all(self.cursor);
        if events.is_empty() {
            if wait {
                let waiter = stream.waiter.clone();
                let notified = waiter.notified();
                drop(stream);
                notified.await;
            }
            Ok(None)
        } else {
            self.events = events;
            self.cursor = stream.end;
            Ok(self.events.pop_front())
        }
    }

    /// Seeks to the given sequence.
    pub async fn seek(&mut self, sequence: u64) -> Result<()> {
        let stream = self.stream.inner.core.lock().await;
        if sequence < stream.start {
            Err(Error::InvalidArgument(format!(
                "seek sequence (is {}) should be >= start (is {})",
                sequence, stream.start
            )))
        } else {
            self.cursor = sequence;
            self.events.clear();
            Ok(())
        }
    }

    /// Returns the next entry or `None` if no available entries.
    pub async fn try_next(&mut self) -> Result<Option<Box<[u8]>>> {
        self.next(false).await
    }

    /// Returns the next entry or wait until it is available.
    pub async fn wait_next(&mut self) -> Result<Box<[u8]>> {
        loop {
            if let Some(next) = self.next(true).await? {
                return Ok(next);
            }
        }
    }
}

pub struct EpochStateStream {
    returned: bool,
}

impl EpochStateStream {
    fn new() -> Self {
        EpochStateStream { returned: false }
    }
}

impl FutureStream for EpochStateStream {
    type Item = EpochState;

    #[allow(unused)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.returned {
            Poll::Pending
        } else {
            this.returned = true;
            Poll::Ready(Some(EpochState {
                epoch: 1,
                role: Role::Leader,
                leader: Some("".into()),
            }))
        }
    }
}
