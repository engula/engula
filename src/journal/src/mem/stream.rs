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

use std::{collections::VecDeque, sync::Arc};

use futures::stream;
use tokio::sync::Mutex;

use crate::{async_trait, Error, Event, Result, ResultStream, Stream, Timestamp};

#[derive(Clone)]
pub struct MemStream<T: Timestamp> {
    events: Arc<Mutex<VecDeque<Event<T>>>>,
}

impl<T: Timestamp> Default for MemStream<T> {
    fn default() -> MemStream<T> {
        MemStream {
            events: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

#[async_trait]
impl<T: Timestamp> Stream<T> for MemStream<T> {
    async fn read_events(&self, ts: T) -> ResultStream<Event<T>> {
        let events = self.events.lock().await;
        let index = events.partition_point(|x| x.ts < ts);
        let iter = events
            .range(index..)
            .cloned()
            .map(Ok)
            .collect::<Vec<Result<Event<T>>>>();
        Box::new(stream::iter(iter))
    }

    async fn append_event(&self, ts: T, data: Vec<u8>) -> Result<()> {
        let event = Event { ts, data };
        let mut events = self.events.lock().await;
        if let Some(last_ts) = events.back().map(|x| x.ts) {
            if ts <= last_ts {
                return Err(Error::InvalidArgument(format!(
                    "timestamp {:?} <= last timestamp {:?}",
                    ts, last_ts
                )));
            }
        }
        events.push_back(event);
        Ok(())
    }

    async fn release_events(&self, ts: T) -> Result<()> {
        let mut events = self.events.lock().await;
        let index = events.partition_point(|x| x.ts < ts);
        events.drain(..index);
        Ok(())
    }
}
