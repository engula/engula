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
    collections::{hash_map, HashMap, VecDeque},
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{async_trait, Error, Event, Result, Timestamp};

type Stream<T> = Arc<Mutex<VecDeque<Event<T>>>>;

#[derive(Clone)]
pub struct Journal<T> {
    streams: Arc<Mutex<HashMap<String, Stream<T>>>>,
}

impl<T> Default for Journal<T> {
    fn default() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T> Journal<T> {
    async fn stream(&self, name: &str) -> Option<Stream<T>> {
        let streams = self.streams.lock().await;
        streams.get(name).cloned()
    }
}

#[async_trait]
impl<T: Timestamp> crate::Journal<T> for Journal<T> {
    type StreamReader = StreamReader<T>;
    type StreamWriter = StreamWriter<T>;

    async fn create_stream(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.lock().await;
        match streams.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                let stream = Arc::new(Mutex::new(VecDeque::new()));
                ent.insert(stream);
                Ok(())
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("stream '{}'", ent.key())))
            }
        }
    }

    async fn delete_stream(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.lock().await;
        match streams.remove(name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }

    async fn new_stream_reader(&self, name: &str) -> Result<Self::StreamReader> {
        if let Some(stream) = self.stream(name).await {
            Ok(StreamReader::new(stream))
        } else {
            Err(Error::NotFound(format!("stream '{}'", name)))
        }
    }

    async fn new_stream_writer(&self, name: &str) -> Result<Self::StreamWriter> {
        if let Some(stream) = self.stream(name).await {
            Ok(StreamWriter::new(stream))
        } else {
            Err(Error::NotFound(format!("stream '{}'", name)))
        }
    }
}

pub struct StreamReader<T> {
    stream: Stream<T>,
    events: VecDeque<Event<T>>,
}

impl<T> StreamReader<T> {
    fn new(stream: Stream<T>) -> Self {
        Self {
            stream,
            events: VecDeque::new(),
        }
    }
}

#[async_trait]
impl<T: Timestamp> crate::StreamReader<T> for StreamReader<T> {
    async fn seek(&mut self, ts: T) -> Result<()> {
        let stream = self.stream.lock().await;
        let offset = stream.partition_point(|x| x.ts < ts);
        self.events = stream.range(offset..).cloned().collect();
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Event<T>>> {
        Ok(self.events.pop_front())
    }
}

pub struct StreamWriter<T> {
    stream: Stream<T>,
}

impl<T> StreamWriter<T> {
    fn new(stream: Stream<T>) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl<T: Timestamp> crate::StreamWriter<T> for StreamWriter<T> {
    async fn append(&mut self, event: Event<T>) -> Result<()> {
        let mut stream = self.stream.lock().await;
        if let Some(last_ts) = stream.back().map(|x| x.ts.clone()) {
            if event.ts <= last_ts {
                return Err(Error::InvalidArgument(format!(
                    "timestamp {:?} <= last timestamp {:?}",
                    event.ts, last_ts
                )));
            }
        }
        stream.push_back(event);
        Ok(())
    }

    async fn release(&mut self, ts: T) -> Result<()> {
        let mut stream = self.stream.lock().await;
        let index = stream.partition_point(|x| x.ts < ts);
        stream.drain(..index);
        Ok(())
    }
}
