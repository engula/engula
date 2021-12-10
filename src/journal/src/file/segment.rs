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

use crate::{file::codec::Codec, Error, Event, Result, Timestamp};

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
        let buf_size = (end - start) as usize;

        let mut reader = self.reader.lock().await;

        reader.seek(SeekFrom::Start(start)).await?;
        let mut buf = vec![0u8; buf_size];
        let n = reader.read(&mut buf).await?;

        if n != buf_size {
            return Err(Error::Unknown(format!(
                "not read enough data of segment {:?} with start {:?} and end {:?}",
                self.path, start, end
            )));
        }

        match Codec::decode_index(&buf) {
            Ok(indexes) => {
                if self.start_index.is_none() {
                    self.start_index = indexes.get(0).cloned();
                }
                self.end_index = indexes.last().cloned();
                Ok(indexes)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn read(&self, start: u64, end: u64) -> Result<Vec<Event>> {
        let buf_size = (end - start) as usize;

        let mut reader = self.reader.lock().await;
        reader.seek(SeekFrom::Start(start)).await?;

        let mut buf = vec![0u8; buf_size as usize];
        let n = reader.read(&mut buf).await?;

        if n != buf_size {
            return Err(Error::Unknown(format!(
                "not read enough data {:?} with start {:?} and end {:?}",
                self.path, start, end
            )));
        }

        Codec::decode_event(&buf)
    }

    pub async fn write(&mut self, event: Event) -> Result<Index> {
        let mut writer = self.writer.lock().await;
        let ts = event.ts;
        let buf = Codec::encode(event);

        writer.write(&buf).await?;
        writer.flush().await?;

        let index = Index {
            ts,
            location: self.position,
            size: buf.len() as u64,
        };

        if self.start_index.is_none() {
            self.start_index = Some(index.clone());
        }
        self.end_index = Some(index.clone());
        self.position += index.size;

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
