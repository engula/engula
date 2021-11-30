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

use std::path::{Path, PathBuf};

use tokio::{fs, io};
use tokio::sync::Mutex;


use super::error::{Error, Result};
use crate::{async_trait, Event, Stream, Timestamp};


#[derive(Clone)]
pub struct FileStream<T: Timestamp> {
    root: PathBuf,

}

impl<T: Timestamp> FileStream<T> {
    pub fn new(root: impl Into<PathBuf>) -> Result<FileStream<T>> {
        let path = root.into();
        fs::DirBuilder::new().recursive(true).create(&path).await?;
        match fs::DirBuilder::new().recursive(true).create(&path).await {
            Ok(_) => {
                let mut stream = FileStream {
                    root: path,
                };
                stream.init();
                Ok(stream)
            }
            Err(e) => Error::IO(e),
        }
    }

    pub fn init(&self) -> Result<()> {

    }

    pub fn clean(&self) -> Result<()> {
        fs::remove_dir_all(self.root.clone()).await?;
        Ok(())
    }

}

#[async_trait]
impl<T: Timestamp> Stream for FileStream<T> {
    type Error = Error;
    type Timestamp = T;
    type EventStream = EventStream<Self::Timestamp>;

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




