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

use tokio::{fs};
use tokio::sync::Mutex;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom};

use crate::{async_trait, Event, Error, ResultStream, Timestamp, Result};
use std::{collections::VecDeque, sync::Arc};

#[derive(Clone)]
pub struct Stream {
    root: PathBuf,
    delete_path: PathBuf,
    index: Arc<Mutex<VecDeque<Index>>>,
    segments: Arc<Mutex<VecDeque<Segment>>>,
}

impl Stream {
    pub async fn new(root: PathBuf) -> Result<Stream> {
        let path = root.clone();
        fs::DirBuilder::new().recursive(true).create(&path).await?;
        match fs::DirBuilder::new().recursive(true).create(&path).await {
            Ok(_) => {

                let segments = Arc::new(Mutex::new(VecDeque::new()));
                let index = Arc::new(Mutex::new(VecDeque::new()));
                let delete_path = Stream::delete_file_path(path.clone());
                let stream = Stream {
                    root: path,
                    delete_path,
                    index,
                    segments
                };
                Ok(stream)
            }
            Err(e) => Err(Error::Unknown(e.to_string()))
        }
    }

    fn active_segment_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root. join(name)
    }

    fn delete_file_path(dir: PathBuf) -> PathBuf {
        dir.join("delete_timestamp")
    }

    pub async fn clean(&self) -> Result<()> {
        fs::remove_dir_all(self.root.clone()).await?;
        Ok(())
    }

    async fn read_events_internal(&self, ts: Timestamp) -> Result<ResultStream<Vec<Event>>> {

        let mut indexes = self.index.lock().await;
        let mut segments = self.segments.lock().await;

        let offset = indexes.partition_point(|x| x.ts < ts);

        let index = indexes.get(offset).ok_or(Err(Error::Unknown(format!("segment not found {:?}", ts))))?;

        let mut events = Vec::new();

        for i in (0..segments.len()).rev() {
            let mut segment = segments.get(i).ok_or(Err(Error::Unknown(format!("segment not found {:?}", ts))))?;
            let start = segment.start?;
            let end = segment.end?;

            if end.ts < index.ts {
                continue;
            }

            if start.ts < index.ts {
                events.push(segment.read(index.location, end.location + end.size)?);
            } else {
                events.push(segment.read(start.location, end.location + end.size)?);
            }
        }
        Ok(ResultStream::new(events))
    }
}

#[async_trait]
impl crate::Stream for Stream {

    async fn read_events(&self, ts: Timestamp) -> ResultStream<Vec<Event>> {
        let output = self.read_events_internal(ts).await;
        match output {
            Ok(output) => output,
            Err(e) => Box::new(futures::stream::once(futures::future::err(e))),
        }
    }
    async fn append_event(&self, event: Event) -> Result<()> {
        let entry = Entry::from(event);

        let mut indexes = self.index.lock().await;
        let mut segments = self.segments.lock().await;

        let mut active = segments.get(0)?;

        match active {
            None => {
                let segment_path = self.active_segment_path(event.ts.into());
                segments.push_front(Segment::create(segment_path).await?);
                segments.get(0)
            }
            Some(segment) => {
                if segment.remaining() < entry.size {
                    let segment_path = self.active_segment_path(event.ts.into());
                    segments.push_front(Segment::create(segment_path).await?);
                    active = segments.get(0)?;
                } else {
                    active = segment;
                }
            }
        };

        let index = active.write(entry)?;
        indexes.push_back(index);
        Ok(())
    }

    async fn release_events(&self, ts: Timestamp) -> Result<()> {
        let mut events = self.events.lock().await;
        let index = events.partition_point(|x| x.ts < ts);
        events.drain(..index);

        let mut indexes = self.index.lock().await;
        let mut segments = self.segments.lock().await;

        let offset = indexes.partition_point(|x| x.ts < ts);
        let index = indexes.get(offset)?;

        for i in (0..segments.len()).rev() {
            let mut segment = segments.get(i)?;
            let end = segment.end?;
            if end < index.ts {
                segment.clean().await;
                segments.drain(i..i + 1);
            }
        }

        let mut time_buf = [0 as u8; 8];
        let time_bytes = ts.to_be_bytes();
        time_buf.clone_from_slice(&time_bytes);

        // create will truncate old content
        let delete_file = File::create(&self.delete_path)?;
        let mut delete_writer = BufWriter::new(delete_file);
        delete_writer.write(&time_buf);

        indexes.drain(..index);

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    pub ts: Timestamp,
    pub path: PathBuf,
    pub location: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub struct Segment {
    pub path: PathBuf,
    pub writer: BufWriter<File>,
    pub reader: BufReader<File>,
    pub start: Option<Index>,
    pub end: Option<Index>,
    pub size: u64,
    pub limit: u64
}

impl Clone for Segment {
    fn clone(&self) -> Self {
        todo!()
    }

    fn clone_from(&mut self, source: &Self) {
        todo!()
    }
}

impl Segment {
    pub async fn create<P>(path: PathBuf) -> Result<Segment> {
        let mut file = File::open(path).await?;
        let mut writer = BufWriter::new(file.try_clone().await?);
        let mut reader = BufReader::new(file.try_clone().await?);

        let segment = Segment {
            path: path.as_ref().to_path_buf(),
            writer,
            reader,
            start: None,
            end: None,
            size: 0,
            limit: 1024 * 1024 * 10 // 10MB
        };

        Ok(segment)
    }

    pub fn remaining(&self) -> u64 {
        return self.limit - self.size
    }

    pub fn read(&mut self, start :u64, end :u64) -> Result<Vec<Event>> {
        let buf_size = ((end - start) / 8) as usize;
        self.reader.seek(SeekFrom::Start(start));
        let mut buf = vec![0u8; buf_size];
        self.reader.read(&mut buf);

        let mut ret = Vec::new();

        let mut i = 0;
        loop {

            if i >= buf_size {
                break;
            }

            let size = buf[i..(i+3)].into();
            let time = Timestamp::deserialize(buf[(i+4) .. i+7].to_vec())?;
            let data : [u8] = buf[(i + 7) .. (i + size)].into();
            ret.push(Event{
                ts: time,
                data: data.to_vec(),
            });
            i += size;
        }

        return Ok(ret);
    }

    pub fn write(&mut self, entry : Entry) -> Result<Index> {

        let mut size_buf = [0; 8];
        let size_bytes = entry.size.to_be_bytes();
        size_buf.clone_from_slice(&size_bytes);

        let mut time_buf = [0; 8];
        let time_bytes = entry.time;
        time_buf.clone_from_slice(&time_bytes);

        let current_pos = self.writer.seek(SeekFrom::Start(0));

        self.writer.write(&size_buf);
        self.writer.write(&time_buf);
        self.writer.write(&entry.data);
        self.writer.flush();

        let index = Index{
            ts: Timestamp::deserialize(entry.time.clone())?,
            path: self.path.clone(),
            location: current_pos.into(),
            size: entry.size
        };

        if self.start.is_none() {
            self.start = Some(index.clone());
        }
        self.end = Some(index.clone());

        return Ok(index);
    }

    pub async fn clean(&mut self) -> Result<()> {
        fs::remove_file(&self.path).await?;
        Ok(())
    }

}
