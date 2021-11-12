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

use std::collections::HashMap;

use futures::stream;
use tokio::sync::Mutex;

use super::stream::MemStream;
use crate::{async_trait, Error, Journal, Result, ResultStream, Stream};

pub struct MemJournal {
    streams: Mutex<HashMap<String, MemStream>>,
}

impl Default for MemJournal {
    fn default() -> Self {
        MemJournal {
            streams: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Journal for MemJournal {
    async fn stream(&self, name: &str) -> Result<Box<dyn Stream>> {
        let streams = self.streams.lock().await;
        match streams.get(name) {
            Some(stream) => Ok(Box::new(stream.clone())),
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }

    async fn list_streams(&self) -> ResultStream<String> {
        let streams = self.streams.lock().await;
        let stream_names = streams
            .keys()
            .cloned()
            .map(Ok)
            .collect::<Vec<Result<String>>>();
        Box::new(stream::iter(stream_names))
    }

    async fn create_stream(&self, name: &str) -> Result<Box<dyn Stream>> {
        let stream = MemStream::default();
        let mut streams = self.streams.lock().await;
        match streams.try_insert(name.to_owned(), stream) {
            Ok(v) => Ok(Box::new(v.clone())),
            Err(_) => Err(Error::AlreadyExists(format!("stream '{}'", name))),
        }
    }

    async fn delete_stream(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.lock().await;
        match streams.remove(name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }
}
