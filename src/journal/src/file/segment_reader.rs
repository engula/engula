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
    io::SeekFrom,
    path::{Path, PathBuf},
};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::segment_stream::SegmentStream;
use crate::{Error, Event, Result, ResultStream, Timestamp};

pub struct SegmentReader {
    path: PathBuf,
    max_offset: u64,
    max_timestamp: Timestamp,
}

impl SegmentReader {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut file = File::open(&path).await?;
        let mut max_offset = file.metadata().await?.len();
        // Reads the max timestamp from the file footer.
        if max_offset < 4 {
            return Err(Error::Corrupted("file size too small".to_owned()));
        }
        max_offset -= 4;
        file.seek(SeekFrom::Start(max_offset)).await?;
        let ts_len = file.read_u32().await?;
        if max_offset < ts_len as u64 {
            return Err(Error::Corrupted("file size too small".to_owned()));
        }
        max_offset -= ts_len as u64;
        file.seek(SeekFrom::Start(max_offset)).await?;
        let mut ts_buf = vec![0; ts_len as usize];
        file.read_exact(&mut ts_buf).await?;
        let max_timestamp = Timestamp::deserialize(ts_buf)?;
        Ok(Self {
            path,
            max_offset,
            max_timestamp,
        })
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn max_timestamp(&self) -> Timestamp {
        self.max_timestamp
    }

    pub async fn read_events(&self, ts: Timestamp) -> Result<ResultStream<Vec<Event>>> {
        SegmentStream::open(&self.path, self.max_offset, Some(ts)).await
    }
}
