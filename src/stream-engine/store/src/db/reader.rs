// Copyright 2022 The Engula Authors.
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
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use stream_engine_proto::ReadResponse;

use super::stream_db::StreamMixin;
use crate::Entry;

pub struct SegmentReader {
    required_epoch: u32,
    next_index: u32,
    limit: usize,
    finished: bool,
    require_acked: bool,

    cached_entries: VecDeque<(u32, Entry)>,

    stream: StreamMixin,
}

impl SegmentReader {
    pub(crate) fn new(
        required_epoch: u32,
        next_index: u32,
        limit: usize,
        require_acked: bool,
        stream: StreamMixin,
    ) -> Self {
        SegmentReader {
            required_epoch,
            next_index,
            limit,
            require_acked,
            stream,
            finished: false,
            cached_entries: VecDeque::new(),
        }
    }

    fn take_cached_entry(&mut self) -> Option<ReadResponse> {
        if let Some((index, entry)) = self.cached_entries.pop_front() {
            // is end of segment?
            if let Entry::Bridge { epoch: _ } = &entry {
                self.finished = true;
            }
            self.next_index = index + 1;
            self.limit -= 1;
            if self.limit == 0 {
                self.finished = true;
            }

            Some(ReadResponse {
                index,
                entry: Some(entry.into()),
            })
        } else {
            None
        }
    }
}

impl Stream for SegmentReader {
    type Item = std::result::Result<ReadResponse, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.finished {
                return Poll::Ready(None);
            }

            if let Some(resp) = this.take_cached_entry() {
                return Poll::Ready(Some(Ok(resp)));
            }

            match this.stream.poll_entries(
                cx,
                this.required_epoch,
                this.next_index,
                this.limit,
                this.require_acked,
            ) {
                Err(err) => {
                    this.finished = true;
                    return Poll::Ready(Some(Err(err.into())));
                }
                Ok(None) => {
                    return Poll::Pending;
                }
                Ok(Some(cached_entries)) => {
                    if cached_entries.is_empty() {
                        this.finished = true;
                    } else {
                        this.cached_entries = cached_entries;
                    }
                }
            }
        }
    }
}
