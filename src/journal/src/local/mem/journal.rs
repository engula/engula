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

#[derive(Default)]
struct Events {
    events: VecDeque<Vec<u8>>,
    last_sequence: Sequence,
    waiters: Vec<Arc<Notify>>,
}

type Stream = Arc<Mutex<Events>>;

impl Events {
    fn read_all_events(&self, sequence: Sequence) -> Option<VecDeque<Vec<u8>>> {
        let next_sequence = self.last_sequence + 1;
        if let Some(offset) = next_sequence.checked_sub(sequence) {
            let index = self.events.len().saturating_sub(offset as usize);
            Some(self.events.range(index..).cloned().collect())
        } else {
            None
        }
    }
}

pub struct Journal {
    streams: Mutex<HashMap<String, Stream>>,
}

impl Default for Journal {
    fn default() -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
        }
    }
}

impl Journal {
    async fn stream(&self, name: &str) -> Option<Stream> {
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
                let stream = Arc::new(Mutex::new(Events::default()));
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

pub struct StreamReader {
    next_sequence: Sequence,
    stream: Stream,
    events: VecDeque<Vec<u8>>,
}

impl StreamReader {
    fn new(stream: Stream) -> Self {
        Self {
            next_sequence: 1,
            stream,
            events: VecDeque::new(),
        }
    }

    async fn advance(&mut self, wait: bool) -> bool {
        let mut stream = self.stream.lock().await;
        match stream.read_all_events(self.next_sequence) {
            Some(events) if !events.is_empty() => {
                self.events = events;
                self.next_sequence = stream.last_sequence + 1;
                true
            }
            _ => {
                if wait {
                    let n = Arc::new(Notify::new());
                    stream.waiters.push(n.clone());
                    drop(stream);
                    n.notified().await;
                }
                false
            }
        }
    }
}

#[async_trait]
impl crate::StreamReader for StreamReader {
    async fn seek(&mut self, sequence: Sequence) -> Result<()> {
        self.next_sequence = sequence;
        self.events.clear();
        Ok(())
    }

    async fn try_next(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(event) = self.events.pop_front() {
            Ok(Some(event))
        } else if self.advance(false).await {
            Ok(self.events.pop_front())
        } else {
            Ok(None)
        }
    }

    async fn wait_next(&mut self) -> Result<Vec<u8>> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Ok(event);
            } else if self.advance(true).await {
                return Ok(self.events.pop_front().unwrap());
            }
        }
    }
}

pub struct StreamWriter {
    stream: Stream,
}

impl StreamWriter {
    fn new(stream: Stream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl crate::StreamWriter for StreamWriter {
    async fn append(&mut self, event: Vec<u8>) -> Result<Sequence> {
        let mut stream = self.stream.lock().await;
        stream.events.push_back(event);
        stream.last_sequence += 1;
        for waiter in &stream.waiters {
            waiter.notify_one();
        }
        stream.waiters.clear();
        Ok(stream.last_sequence)
    }

    async fn truncate(&mut self, sequence: Sequence) -> Result<()> {
        let mut stream = self.stream.lock().await;
        let next_sequence = stream.last_sequence + 1;
        if let Some(offset) = next_sequence.checked_sub(sequence) {
            let index = stream.events.len().saturating_sub(offset as usize);
            stream.events.drain(..index);
        } else {
            stream.events.clear();
        }
        Ok(())
    }
}
