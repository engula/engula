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

use std::{path::PathBuf, sync::Arc};

use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom},
    sync::Mutex,
};

use crate::{Event, Result, Timestamp};

#[derive(Clone, Debug)]
pub struct Segment {
    pub path: PathBuf,
    pub writer: Arc<Mutex<BufWriter<File>>>,
    pub reader: Arc<Mutex<BufReader<File>>>,
    pub start_index: Option<Index>,
    pub end_index: Option<Index>,
    pub limit: u64,
    pub position: u64,
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

            if self.start_index.is_none() {
                self.start_index = Some(index.clone());
            }
            self.end_index = Some(index.clone());

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

        if self.start_index.is_none() {
            self.start_index = Some(index.clone());
        }
        self.end_index = Some(index.clone());
        self.position += entry.size;

        Ok(index)
    }

    pub async fn clean(&mut self) -> Result<()> {
        fs::remove_file(&self.path).await?;
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
