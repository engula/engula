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
use tokio::fs::File;

use super::codec;
use crate::{Event, Result, ResultStream, Timestamp};

pub struct SegmentStream {
    file: File,
    offset: usize,
    max_offset: usize,
    start_event: Option<Event>,
}

impl SegmentStream {
    pub async fn open(
        path: impl AsRef<Path>,
        limit: usize,
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
        if let Some((event, offset)) =
            codec::read_event_at(&mut self.file, self.offset, self.max_offset).await?
        {
            self.offset = offset;
            Ok(Some(event))
        } else {
            Ok(None)
        }
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
