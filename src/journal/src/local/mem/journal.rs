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

use engula_futures::stream::batch::VecResultStream;
use tokio::sync::{Mutex, Notify};

use crate::{async_trait, Error, Result, Sequence};

type SeekableEventStream = Arc<Mutex<SeekableEventStreamInner>>;

#[derive(Default)]
struct SeekableEventStreamInner {
    events: VecDeque<Vec<u8>>,
    start: Sequence,
    end: Sequence,
    waiter: Arc<Notify>,
}

impl SeekableEventStreamInner {
    fn read_all(&self, from: Sequence) -> VecDeque<Vec<u8>> {
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

#[derive(Default)]
pub struct Journal {
    streams: Mutex<HashMap<String, SeekableEventStream>>,
}

impl Journal {
    async fn stream(&self, name: &str) -> Option<SeekableEventStream> {
        let streams = self.streams.lock().await;
        streams.get(name).cloned()
    }
}

#[async_trait]
impl crate::Journal for Journal {
    type StreamLister = VecResultStream<String, Error>;
    type StreamReader = StreamReader;
    type StreamWriter = StreamWriter;

    async fn list_streams(&self) -> Result<Self::StreamLister> {
        let streams = self.streams.lock().await;
        Ok(VecResultStream::new(streams.keys().cloned().collect()))
    }

    async fn create_stream(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.lock().await;
        match streams.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(SeekableEventStream::default());
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

pub struct StreamReader {
    cursor: Sequence,
    stream: SeekableEventStream,
    events: VecDeque<Vec<u8>>,
}

impl StreamReader {
    fn new(stream: SeekableEventStream) -> Self {
        Self {
            cursor: 0,
            stream,
            events: VecDeque::new(),
        }
    }

    async fn next(&mut self, wait: bool) -> Result<Option<(Sequence, Vec<u8>)>> {
        if let Some(event) = self.events.pop_front() {
            let sequence = self.cursor;
            self.cursor += 1;
            return Ok(Some((sequence, event)));
        }

        let stream = self.stream.lock().await;
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
            let sequence = self.cursor;
            self.cursor += 1;
            Ok(self.events.pop_front().map(|x| (sequence, x)))
        }
    }
}

#[async_trait]
impl crate::StreamReader for StreamReader {
    async fn seek(&mut self, sequence: Sequence) -> Result<()> {
        let stream = self.stream.lock().await;
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

    async fn try_next(&mut self) -> Result<Option<(Sequence, Vec<u8>)>> {
        self.next(false).await
    }

    async fn wait_next(&mut self) -> Result<(Sequence, Vec<u8>)> {
        loop {
            if let Some(next) = self.next(true).await? {
                return Ok(next);
            }
        }
    }
}

pub struct StreamWriter {
    stream: SeekableEventStream,
}

impl StreamWriter {
    fn new(stream: SeekableEventStream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl crate::StreamWriter for StreamWriter {
    async fn append(&mut self, event: Vec<u8>) -> Result<Sequence> {
        let mut stream = self.stream.lock().await;
        stream.events.push_back(event);
        stream.end += 1;
        stream.waiter.notify_waiters();
        Ok(stream.end - 1)
    }

    async fn truncate(&mut self, sequence: Sequence) -> Result<()> {
        let mut stream = self.stream.lock().await;
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
