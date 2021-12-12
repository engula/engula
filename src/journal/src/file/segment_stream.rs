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

use std::path::Path;

use futures::stream;
use tokio::{fs::File, io::AsyncReadExt};

use crate::{Error, Event, Result, ResultStream, Timestamp};

pub struct SegmentStream {
    file: File,
    offset: u64,
    max_offset: u64,
    start_event: Option<Event>,
}

impl SegmentStream {
    pub async fn open(
        path: impl AsRef<Path>,
        limit: u64,
        start_ts: Option<Timestamp>,
    ) -> Result<ResultStream<Vec<Event>>> {
        let file = File::open(path).await?;
        let mut stream = Self {
            file,
            offset: 0,
            max_offset: limit,
            start_event: None,
        };

        // Seeks to the start event.
        if let Some(ts) = start_ts {
            while let Some(event) = stream.read_event().await? {
                if event.ts >= ts {
                    stream.start_event = Some(event);
                    break;
                }
            }
        }

        let stream = stream::unfold(stream, |mut stream| async move {
            stream.next_events().await.map(|events| (events, stream))
        });
        Ok(Box::pin(stream))
    }

    async fn read_event(&mut self) -> Result<Option<Event>> {
        if self.offset == self.max_offset {
            return Ok(None);
        }
        self.offset += 8;
        if self.offset > self.max_offset {
            return Err(Error::Corrupted(format!(
                "offset {} > max_offset {}",
                self.offset, self.max_offset
            )));
        }
        if self.offset == self.max_offset {
            return Ok(None);
        }
        let ts_len = self.file.read_u32().await?;
        let data_len = self.file.read_u32().await?;
        self.offset += (ts_len + data_len) as u64;
        if self.offset > self.max_offset {
            return Err(Error::Corrupted(format!(
                "offset {} > max_offset {}",
                self.offset, self.max_offset
            )));
        }
        let mut ts_buf = vec![0; ts_len as usize];
        self.file.read_exact(&mut ts_buf).await?;
        let ts = Timestamp::deserialize(ts_buf)?;
        let mut data = vec![0; data_len as usize];
        self.file.read_exact(&mut data).await?;
        Ok(Some(Event { ts, data }))
    }

    async fn next_event(&mut self) -> Result<Option<Event>> {
        if let Some(event) = self.start_event.take() {
            Ok(Some(event))
        } else {
            self.read_event().await
        }
    }

    async fn next_events(&mut self) -> Option<Result<Vec<Event>>> {
        match self.next_event().await {
            Ok(Some(event)) => Some(Ok(vec![event])),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
