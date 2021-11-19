// Copyright 2021 The Engula Authors.
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

use tokio::sync::Mutex;

use super::error::{Error, Result};
use crate::{async_trait, Event, Stream, Timestamp};

#[derive(Clone)]
pub struct MemStream<T: Timestamp> {
    events: Arc<Mutex<VecDeque<Event<T>>>>,
}

impl<T: Timestamp> Default for MemStream<T> {
    fn default() -> Self {
        MemStream {
            events: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

#[async_trait]
impl<T: Timestamp> Stream for MemStream<T> {
    type Error = Error;
    type EventStream = EventStream<Self::Timestamp>;
    type Timestamp = T;

    async fn read_events(&self, ts: Self::Timestamp) -> Result<Self::EventStream> {
        let events = self.events.lock().await;
        let offset = events.partition_point(|x| x.ts < ts);
        Ok(EventStream::new(events.range(offset..).cloned().collect()))
    }

    async fn append_event(&self, event: Event<Self::Timestamp>) -> Result<()> {
        let mut events = self.events.lock().await;
        if let Some(last_ts) = events.back().map(|x| x.ts) {
            if event.ts <= last_ts {
                return Err(Error::InvalidArgument(format!(
                    "timestamp {:?} <= last timestamp {:?}",
                    event.ts, last_ts
                )));
            }
        }
        events.push_back(event);
        Ok(())
    }

    async fn release_events(&self, ts: Self::Timestamp) -> Result<()> {
        let mut events = self.events.lock().await;
        let index = events.partition_point(|x| x.ts < ts);
        events.drain(..index);
        Ok(())
    }
}

pub struct EventStream<T: Timestamp> {
    events: Vec<Event<T>>,
    offset: usize,
}

impl<T: Timestamp> EventStream<T> {
    fn new(events: Vec<Event<T>>) -> Self {
        EventStream { events, offset: 0 }
    }
}

impl<T: Timestamp> futures::Stream for EventStream<T> {
    type Item = Result<Event<T>>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.offset == self.events.len() {
            Poll::Ready(None)
        } else {
            self.as_mut().get_mut().offset += 1;
            Poll::Ready(Some(Ok(self.events[self.offset - 1].clone())))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.events.len() - self.offset;
        (size, Some(size))
    }
}
