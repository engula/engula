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
    collections::{hash_map, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;

use super::stream::Stream;
use crate::{async_trait, Error, Result};

#[derive(Clone)]
pub struct Journal {
    streams: Arc<Mutex<HashMap<String, Stream>>>,
}

impl Default for Journal {
    fn default() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl crate::Journal for Journal {
    type Stream = Stream;

    async fn stream(&self, name: &str) -> Result<Self::Stream> {
        let streams = self.streams.lock().await;
        match streams.get(name) {
            Some(stream) => Ok(stream.clone()),
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }

    async fn create_stream(&self, name: &str) -> Result<Self::Stream> {
        let stream = Stream::default();
        let mut streams = self.streams.lock().await;
        match streams.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(stream.clone());
                Ok(stream)
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
