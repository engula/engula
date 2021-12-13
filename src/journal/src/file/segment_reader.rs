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

use tokio::fs::File;

use super::{codec, segment_stream::SegmentStream};
use crate::{Event, Result, ResultStream, Timestamp};

pub struct SegmentReader {
    path: PathBuf,
    max_offset: usize,
    max_timestamp: Timestamp,
}

impl SegmentReader {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut file = File::open(&path).await?;
        let max_offset = file.metadata().await?.len() as usize;
        // Reads the max timestamp from the file footer.
        let (max_timestamp, max_offset) = codec::read_footer_from(&mut file, max_offset).await?;
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
