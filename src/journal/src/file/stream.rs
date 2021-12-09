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
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::Mutex,
};

use crate::{
    async_trait,
    file::segment::{Entry, Index, Segment},
    Error, Event, Result, ResultStream, Timestamp,
};

const DELETE_FILE_NAME: &str = "delete_timestamp";

#[derive(Clone)]
pub struct Stream {
    root: PathBuf,
    delete_path: PathBuf,
    index: Arc<Mutex<VecDeque<Index>>>,
    segments: Arc<Mutex<VecDeque<Segment>>>,
}

impl Stream {
    pub async fn create(root: PathBuf) -> Result<Stream> {
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

    fn segment_path(&self, name: String) -> PathBuf {
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

        let index = index_option.unwrap();

        let mut events = Vec::new();

        for i in (0..segments.len()).rev() {
            let segment = &mut segments[i];
            let start_index = segment.start_index.as_ref().unwrap();
            let end_index = segment.end_index.as_ref().unwrap();

            if end_index.ts < index.ts {
                continue;
            }

            if start_index.ts < index.ts {
                events.append(
                    segment
                        .read(index.location, end_index.location + end_index.size)
                        .await?
                        .as_mut(),
                );
            } else {
                events.append(
                    segment
                        .read(start_index.location, end_index.location + end_index.size)
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
            start_index: None,
            end_index: None,
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
                let mut segment = self.create_segment(segment_file).await?;

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
                    stream_list.push(child.path())
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
            let segment_path: PathBuf = self.segment_path(format!("{:?}", &entry.time));
            let segment = self.create_segment(segment_path).await?;
            segments.push_front(segment);
        }

        let active_segment = &mut segments[0];
        let index = active_segment.write(&entry).await?;
        indexes.push_back(index);

        // check if need move to another file
        if active_segment.is_full() {
            let segment_path = self.segment_path(format!("{:?}", &entry.time));
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
            let end_index = segment.end_index.as_ref().unwrap();
            if end_index.ts < ts {
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
