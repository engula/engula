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

use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use futures::{future, stream};
use tokio::{
    fs,
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom},
    sync::Mutex,
};

use crate::{async_trait, Error, Event, Result, ResultStream, Timestamp};

const DELETE_FILE_NAME: &str = "delete_timestamp";

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
                let mut stream = Stream {
                    root: path,
                    delete_path,
                    index,
                    segments,
                };
                stream.try_recovery().await?;
                Ok(stream)
            }
            Err(e) => Err(Error::Unknown(e.to_string())),
        }
    }

    fn active_segment_path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn delete_file_path(dir: PathBuf) -> PathBuf {
        dir.join(DELETE_FILE_NAME)
    }

    pub async fn clean(&self) -> Result<()> {
        fs::remove_dir_all(self.root.clone()).await?;
        Ok(())
    }

    async fn read_events_internal(&self, ts: Timestamp) -> Result<ResultStream<Vec<Event>>> {
        let indexes = self.index.lock().await;
        let mut segments = self.segments.lock().await;

        let offset = indexes.partition_point(|x| x.ts < ts);

        let index_option = indexes.get(offset);

        if index_option.is_none() {
            return Ok(Box::new(stream::once(future::ok(Vec::new()))));
        }

        let index = index_option.expect("index out of offset");

        let mut events = Vec::new();

        for i in (0..segments.len()).rev() {
            let segment = &mut segments[i];
            let start = segment.start.as_ref().ok_or("segment no start index")?;
            let end = segment.end.as_ref().ok_or("segment no end index")?;

            if end.ts < index.ts {
                continue;
            }

            if start.ts < index.ts {
                events.append(
                    segment
                        .read(index.location, end.location + end.size)
                        .await?
                        .as_mut(),
                );
            } else {
                events.append(
                    segment
                        .read(start.location, end.location + end.size)
                        .await?
                        .as_mut(),
                );
            }
        }
        Ok(Box::new(stream::once(future::ok(events))))
    }

    async fn create_segment(&self, path: PathBuf) -> Result<Segment> {
        let write_file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(false)
            .append(true)
            .truncate(false)
            .open(path.clone())
            .await?;

        let read_file = OpenOptions::new()
            .write(false)
            .create(false)
            .read(true)
            .append(false)
            .truncate(false)
            .open(path.clone())
            .await?;

        let total_size = read_file.metadata().await?.len();

        let writer = BufWriter::new(write_file);
        let reader = BufReader::new(read_file);

        let segment = Segment {
            path: path.clone(),
            writer: Arc::new(Mutex::new(writer)),
            reader: Arc::new(Mutex::new(reader)),
            start: None,
            end: None,
            limit: 1024 * 1024 * 50, // 50MB
            position: total_size,
        };
        Ok(segment)
    }

    async fn try_recovery(&mut self) -> Result<()> {
        let mut delete_time = None;

        let delete_file_result = File::open(&self.delete_path).await;
        if let Ok(deleted_file) = delete_file_result {
            let mut delete_reader = BufReader::new(deleted_file);
            delete_time = Some(delete_reader.read_u64().await?);
        }

        if let Ok(segment_file) = Stream::read_segment_file(self.root.clone()).await {
            let mut segments = self.segments.lock().await;
            let mut indexes = self.index.lock().await;

            for segment_file in segment_file {
                if let Some(name) = segment_file.file_name() {
                    let segment_path: PathBuf =
                        self.active_segment_path(name.to_str().ok_or("name to str error")?);
                    let mut segment = self.create_segment(segment_path).await?;

                    for index in segment.read_index(0, segment.position).await? {
                        if let Some(delete) = delete_time {
                            if index.ts < Timestamp::from(delete) {
                                continue;
                            }
                        }
                        indexes.push_back(index);
                    }
                    segments.push_front(segment);
                }
            }
        }
        Ok(())
    }

    async fn read_segment_file(root: impl Into<PathBuf>) -> Result<Vec<PathBuf>> {
        let path = root.into();

        let mut stream_list: Vec<PathBuf> = Vec::new();

        let mut dir = fs::read_dir(path).await?;
        while let Some(child) = dir.next_entry().await? {
            if child.metadata().await?.is_file() {
                let name = child.file_name();
                if name.ne(DELETE_FILE_NAME) {
                    stream_list.push(child.file_name().into())
                }
            }
        }
        Ok(stream_list)
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

        if segments.get(0).is_none() {
            let segment_path: PathBuf =
                self.active_segment_path(format!("{:}?", &entry.time).as_str());
            let segment = self.create_segment(segment_path).await?;
            segments.push_front(segment);
        }

        let active = &mut segments[0];
        let index = active.write(&entry).await?;
        indexes.push_back(index);

        // check if need move to another file
        if active.is_full() {
            let segment_path = self.active_segment_path(format!("{:}?", &entry.time).as_str());
            let segment = self.create_segment(segment_path).await?;
            segments.push_front(segment);
        }
        Ok(())
    }

    async fn release_events(&self, ts: Timestamp) -> Result<()> {
        let mut indexes = self.index.lock().await;
        let mut segments = self.segments.lock().await;

        let offset = indexes.partition_point(|x| x.ts < ts);

        for i in (0..segments.len()).rev() {
            let segment = &mut segments[i];
            let end = &segment.end.as_ref().ok_or("segment no end index")?;
            if end.ts < ts {
                segment.clean().await?;
                segments.drain(i..i + 1);
            }
        }

        let mut time_buf = [0_u8; 8];
        let time_bytes = ts.serialize();
        time_buf.clone_from_slice(&time_bytes);

        // create will truncate old content
        let delete_file = File::create(&self.delete_path).await?;
        let mut delete_writer = BufWriter::new(delete_file);
        delete_writer.write(&time_buf).await?;
        delete_writer.flush().await?;

        indexes.drain(..offset);

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    pub ts: Timestamp,
    pub location: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub size: u64,
    pub time: Timestamp,
    pub data: Vec<u8>,
}

impl From<Event> for Entry {
    fn from(event: Event) -> Self {
        Entry {
            size: (8 + event.ts.serialize().len() + event.data.len()) as u64,
            time: event.ts,
            data: event.data,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Segment {
    pub path: PathBuf,
    pub writer: Arc<Mutex<BufWriter<File>>>,
    pub reader: Arc<Mutex<BufReader<File>>>,
    pub start: Option<Index>,
    pub end: Option<Index>,
    pub limit: u64,
    position: u64,
}

impl Segment {
    pub fn is_full(&self) -> bool {
        self.limit <= self.position
    }

    pub async fn read_index(&mut self, start: u64, end: u64) -> Result<Vec<Index>> {
        let buf_size = end - start;

        let mut reader = self.reader.lock().await;

        reader.seek(SeekFrom::Start(start)).await?;
        let mut buf = vec![0u8; buf_size as usize];
        let n = reader.read(&mut buf).await?;
        println!("{}", n);

        let mut ret = Vec::new();

        let mut i = 0_u64;
        loop {
            if i >= buf_size {
                break;
            }
            let size_bytes = &buf[i as usize..(i + 8) as usize];
            let mut size_buf = [0; 8];
            size_buf.clone_from_slice(size_bytes);
            let size = u64::from_be_bytes(size_buf);

            let time_bytes = buf[(i + 8) as usize..(i + 16) as usize].to_vec();
            let time = Timestamp::deserialize(time_bytes)?;

            let index = Index {
                ts: time,
                location: i,
                size,
            };

            if self.start.is_none() {
                self.start = Some(index.clone());
            }
            self.end = Some(index.clone());

            ret.push(index);
            i += size;
        }

        Ok(ret)
    }

    pub async fn read(&self, start: u64, end: u64) -> Result<Vec<Event>> {
        let buf_size = end - start;

        let mut reader = self.reader.lock().await;

        reader.seek(SeekFrom::Start(start)).await?;
        let mut buf = vec![0u8; buf_size as usize];
        let n = reader.read(&mut buf).await?;
        println!("{}", n);

        let mut ret = Vec::new();

        let mut i = 0_u64;
        loop {
            if i >= buf_size {
                break;
            }
            let size_bytes = &buf[i as usize..(i + 8) as usize];
            let mut size_buf = [0; 8];
            size_buf.clone_from_slice(size_bytes);
            let size = u64::from_be_bytes(size_buf);

            let time_bytes = buf[(i + 8) as usize..(i + 16) as usize].to_vec();
            let time = Timestamp::deserialize(time_bytes)?;

            let data = buf[(i + 16) as usize..(i + size) as usize].to_vec();
            ret.push(Event { ts: time, data });
            i += size;
        }

        Ok(ret)
    }

    pub async fn write(&mut self, entry: &Entry) -> Result<Index> {
        let mut writer = self.writer.lock().await;

        let mut size_buf = [0; 8];
        let size_bytes = entry.size.to_be_bytes();
        size_buf.clone_from_slice(&size_bytes);

        let mut time_buf = [0; 8];
        let time_bytes = entry.time.serialize();
        time_buf.clone_from_slice(&time_bytes);

        writer.write(&size_buf).await?;
        writer.write(&time_buf).await?;
        writer.write(&entry.data).await?;
        writer.flush().await?;

        let index = Index {
            ts: entry.time,
            location: self.position,
            size: entry.size,
        };

        if self.start.is_none() {
            self.start = Some(index.clone());
        }
        self.end = Some(index.clone());
        self.position += entry.size;

        Ok(index)
    }

    pub async fn clean(&mut self) -> Result<()> {
        fs::remove_file(&self.path).await?;
        Ok(())
    }
}
