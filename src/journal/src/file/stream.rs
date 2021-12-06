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

use std::io::BufWriter;
use std::path::{Path, PathBuf};

use tokio::{fs, io};
use tokio::sync::Mutex;
use tokio::fs::File;
use tokio::io::{BufReader, BufWriter};


use super::error::{Error, Result};
use crate::{async_trait, Event, ResultStream, Timestamp};
use std::{collections::VecDeque, sync::Arc};

#[derive(Clone)]
pub struct Stream {
    root: PathBuf,
    delete_file: fs::File,
    index: Arc<Mutex<VecDeque<Index>>>,
    segments: Arc<Mutex<VecDeque<Segment>>>,
}

impl Stream {
    pub fn new(root: impl Into<PathBuf>) -> Result<Stream> {
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

    pub fn init(&mut self) -> Result<()> {
        let dir = File::open(&self.root).await?;
        dir.try_lock_exclusive().await?;

        self.segments = Arc::new(Mutex::new(VecDeque::new()));
        self.index = Arc::new(Mutex::new(VecDeque::new()));

        // let mut segments = self.segments.lock().await;
        // lazy init when first write
        // if segments.len() == 0 {
        //     let segment_path = self.active_segment_path();
        //     segments.push_front(Segment::create(segment_path)?);
        // }

        return Ok(())
    }

    fn active_segment_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }

    pub fn clean(&self) -> Result<()> {
        fs::remove_dir_all(self.root.clone()).await?;
        Ok(())
    }
}

#[async_trait]
impl crate::Stream for Stream {

    async fn read_events(&self, ts: Timestamp) -> ResultStream<Vec<Event>> {
        let events = self.events.lock().await;
        let offset = events.partition_point(|x| x.ts < ts);
        Ok(EventStream::new(events.range(offset..).cloned().collect()))
    }

    async fn append_event(&self, event: Event) -> Result<()> {
        let entry = Entry::from(event);

        let mut indexes = self.index.lock().await;
        let mut segments = self.segments.lock().await;


        let mut active = segments.get(0);

        active = match active {
            None => {
                let segment_path = self.active_segment_path(event.ts.into());
                segments.push_front(Segment::create(segment_path)?);
                segments.get(0)
            }
            Some(segment) => {
                if segment.remaining() < entry.size {
                    let segment_path = self.active_segment_path(event.ts.into());
                    segments.push_front(Segment::create(segment_path)?);
                    active = segments.get(0)
                } else {
                    Ok(segment)
                }
            }
        };

        let index = active?.write(entry)?;
        indexes.push_back(index);
        Ok(())
    }

    async fn release_events(&self, ts: Timestamp) -> Result<()> {
        let mut events = self.events.lock().await;
        let index = events.partition_point(|x| x.ts < ts);
        events.drain(..index);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    pub ts: Timestamp,
    pub location: u64,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub size: u64,
    pub time: Vec<u8>,
    pub data: Vec<u8>
}


impl From<Event> for Entry {
    fn from(event: Event) -> Self {
        let  time = event.ts.serialize();

        Entry {
            size: (time.len() + event.data.len()) as u64,
            time,
            data: event.data,
        }
    }
}



#[derive(Clone, Debug, PartialEq)]
pub struct Segment {
    pub path: PathBuf,
    pub writer: fs::io::BufWriter<File>,
    pub reader: fs::io::BufReader<File>,
    pub file : fs::File,
    pub start: Option<Index>,
    pub end: Option<Index>,
    pub size: u64,
    pub limit: u64
}

impl Segment {
    pub fn create<P>(path: PathBuf) -> Result<Segment> {
        let mut file = File::open("active.segment").await?;
        let mut writer = fs::io::BufWriter::new(file.try_clone()?);
        let mut reader = fs::io::BufReader::new(file.try_clone()?);

        let segment = Segment {
            path: path.as_ref().to_path_buf(),
            writer,
            reader,
            file,
            start: None,
            end: None,
            size: 0,
            limit: 1024 * 1024 * 10 // 10MB
        };

        info!("{:?}: created", segment);
        Ok(segment)
    }

    pub fn remaining(&self) -> u64 {
        return self.limit - self.size
    }

    pub fn write(&self, entry : Entry) -> Option<Index> {
        self.writer
    }

}
