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
    sync::Arc,
    task::{Context, Poll},
};

use stream_engine_proto::*;
use tokio::sync::mpsc;

use crate::{
    group::EventChannel, master::Stream as StreamClient, policy::Policy as ReplicatePolicy,
    reader::StreamReader, store::Transport, Engine, Result,
};

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
    pub(crate) fn new(engine: Engine, stream_client: StreamClient, channel: EventChannel) -> Self {
        let inner = StreamInner {
            engine,
            stream_client,
            channel,
            transport: Transport::new(),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    #[inline(always)]
    pub fn desc(&self) -> StreamDesc {
        self.inner.stream_client.desc()
    }

    /// Return the current epoch state of the specified stream.
    pub async fn current_state(&self) -> Result<EpochState> {
        Ok(self.inner.channel.on_epoch_state().await?)
    }

    /// Return a endless stream which returns a new epoch state once the
    /// associated stream enters a new epoch.
    pub async fn subscribe_state(&self) -> Result<EpochStateStream> {
        let stream_id = self.inner.stream_client.stream_id();
        let receiver = self.inner.engine.observe_stream(stream_id).await?;
        Ok(EpochStateStream { receiver })
    }

    /// Returns a stream reader.
    pub async fn new_reader(&self) -> Result<StreamReader> {
        Ok(StreamReader::new(
            ReplicatePolicy::Simple,
            self.inner.stream_client.clone(),
            self.inner.transport.clone(),
        ))
    }

    /// Append an event, returns the sequence.
    pub async fn append(&self, event: Box<[u8]>) -> Result<u64> {
        let sequence = self.inner.channel.on_propose(event).await??;
        Ok(sequence.into())
    }

    /// Truncates events up to a sequence (exclusive).
    pub async fn truncate(&self, sequence: u64) -> Result<()> {
        self.inner.channel.on_truncate(sequence.into()).await?
    }
}

struct StreamInner {
    engine: Engine,
    stream_client: StreamClient,
    transport: Transport,
    channel: EventChannel,
}

#[derive(Debug)]
pub struct EpochStateStream {
    receiver: mpsc::UnboundedReceiver<EpochState>,
}

impl futures::Stream for EpochStateStream {
    type Item = EpochState;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().receiver).poll_recv(cx)
    }
}
