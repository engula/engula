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

use futures::{future, stream, StreamExt};
use tokio::{fs, sync::Mutex};

use super::{segment::Segment, segment_reader::SegmentReader};
use crate::{async_trait, Error, Event, Result, ResultStream, Timestamp};

#[derive(Clone)]
pub struct Stream {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
pub struct Inner {
    path: PathBuf,
    segment_size: u64,
    active_segment: Option<Segment>,
    sealed_segments: Vec<SegmentReader>,
}

const ACTIVE_SEGMENT: &str = "CURRENT";

impl Inner {
    fn active_segment_path(&self) -> PathBuf {
        self.path.join(ACTIVE_SEGMENT)
    }

    fn sealed_segment_path(&self, ts: Timestamp) -> PathBuf {
        self.path.join(format!("{:?}", ts))
    }
}

impl Stream {
    pub async fn open(path: PathBuf, segment_size: u64) -> Result<Stream> {
        let mut inner = Inner {
            path,
            segment_size,
            active_segment: None,
            sealed_segments: Vec::new(),
        };

        // Opens all sealed segments and sorts them by timestamp.
        let mut entries = fs::read_dir(&inner.path).await?;
        while let Some(ent) = entries.next_entry().await? {
            if ent.file_name() != ACTIVE_SEGMENT {
                let segment = SegmentReader::open(ent.path()).await?;
                inner.sealed_segments.push(segment);
            }
        }
        inner.sealed_segments.sort_by_key(|x| x.max_timestamp());

        let last_timestamp = inner.sealed_segments.last().map(|x| x.max_timestamp());
        let active = Segment::open(inner.active_segment_path(), last_timestamp).await?;
        inner.active_segment = Some(active);

        Ok(Stream {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    async fn read_segments(&self, ts: Timestamp) -> Result<Vec<ResultStream<Vec<Event>>>> {
        let inner = self.inner.lock().await;
        let index = inner
            .sealed_segments
            .partition_point(|x| x.max_timestamp() < ts);
        let mut streams = Vec::new();
        for segment in &inner.sealed_segments[index..] {
            streams.push(segment.read_events(ts).await?);
        }
        if let Some(segment) = &inner.active_segment {
            streams.push(segment.read_events(ts).await?);
        }
        Ok(streams)
    }
}

#[async_trait]
impl crate::Stream for Stream {
    async fn read_events(&self, ts: Timestamp) -> ResultStream<Vec<Event>> {
        match self.read_segments(ts).await {
            Ok(streams) => Box::pin(stream::iter(streams).flatten()),
            Err(err) => Box::pin(stream::once(future::err(err))),
        }
    }

    async fn append_event(&self, event: Event) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let size = if let Some(active) = inner.active_segment.as_mut() {
            active.append_event(event).await?
        } else {
            return Err(Error::Unknown(
                "active segment is closed due to previous errors".to_owned(),
            ));
        };

        if size >= inner.segment_size {
            // Seals the active segment.
            let active = inner.active_segment.take().unwrap();
            let last_timestamp = active.seal().await?;

            // Renames the active segment to a sealed segment.
            let active_segment_path = inner.active_segment_path();
            let sealed_segment_path = inner.sealed_segment_path(last_timestamp);
            fs::rename(&active_segment_path, &sealed_segment_path).await?;
            let sealed = SegmentReader::open(sealed_segment_path).await?;
            inner.sealed_segments.push(sealed);

            // Opens a new active segment.
            let active = Segment::open(&active_segment_path, Some(last_timestamp)).await?;
            inner.active_segment = Some(active);
        }
        Ok(())
    }

    async fn release_events(&self, ts: Timestamp) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let index = inner
            .sealed_segments
            .partition_point(|x| x.max_timestamp() < ts);
        for segment in inner.sealed_segments.drain(..index) {
            fs::remove_file(segment.path()).await?;
        }
        Ok(())
    }
}
