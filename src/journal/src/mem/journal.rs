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

use std::collections::{hash_map, HashMap};

use tokio::sync::Mutex;

use super::stream::MemStream;
use crate::{async_trait, Error, Journal, Result, Stream, Timestamp};

pub struct MemJournal<T: Timestamp> {
    streams: Mutex<HashMap<String, MemStream<T>>>,
}

impl<T: Timestamp> Default for MemJournal<T> {
    fn default() -> Self {
        MemJournal {
            streams: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<T: Timestamp> Journal<T> for MemJournal<T> {
    async fn stream(&self, name: &str) -> Result<Box<dyn Stream<T>>> {
        let streams = self.streams.lock().await;
        match streams.get(name) {
            Some(stream) => Ok(Box::new(stream.clone())),
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }

    async fn create_stream(&self, name: &str) -> Result<Box<dyn Stream<T>>> {
        let stream = MemStream::default();
        let mut streams = self.streams.lock().await;
        match streams.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(stream.clone());
                Ok(Box::new(stream))
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
}
