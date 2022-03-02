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

use futures::StreamExt;

use super::segment::SegmentReader;
use crate::{
    master::Stream as MasterStream, policy::Policy as ReplicatePolicy, store::Transport, Error,
    Result, Sequence,
};

#[allow(dead_code)]
pub struct StreamReader {
    policy: ReplicatePolicy,
    transport: Transport,
    master_stream: MasterStream,

    current_epoch: u32,
    start_index: Option<u32>,
    segment_reader: Option<SegmentReader>,
}

#[allow(dead_code)]
impl StreamReader {
    pub(crate) fn new(
        policy: ReplicatePolicy,
        master_stream: MasterStream,
        transport: Transport,
    ) -> Self {
        StreamReader {
            policy,
            transport,
            master_stream,
            current_epoch: 0,
            start_index: None,
            segment_reader: None,
        }
    }

    async fn switch_segment(&mut self) -> Result<()> {
        let segment_meta = self.master_stream.get_segment(self.current_epoch).await?;
        let segment_meta = match segment_meta {
            Some(meta) => meta,
            None => {
                return Err(Error::NotFound(format!(
                    "stream {} segment {}",
                    self.master_stream.stream_id(),
                    self.current_epoch
                )));
            }
        };

        let segment_reader = SegmentReader::new(
            self.policy,
            self.transport.clone(),
            self.master_stream.stream_id(),
            self.current_epoch,
            std::mem::take(&mut self.start_index),
            segment_meta.copy_set,
        )
        .await;
        self.segment_reader = Some(segment_reader);

        Ok(())
    }
}

#[allow(dead_code)]
impl StreamReader {
    /// Seeks to the given sequence.
    pub async fn seek(&mut self, sequence: u64) -> Result<()> {
        let sequence = Sequence::from(sequence);
        self.current_epoch = sequence.epoch;
        self.start_index = Some(sequence.index);
        self.switch_segment().await?;
        Ok(())
    }

    /// Returns the next entry or `None` if no available entries.
    pub async fn try_next(&mut self) -> Result<Option<Box<[u8]>>> {
        // TODO(w41ter) Implementing a try operation is not easy, because the underlying
        // `Streaming` couldn't distinguish whether there is no events or in-flights.
        todo!()
    }

    /// Returns the next entry or wait until it is available.
    pub async fn wait_next(&mut self) -> Result<Box<[u8]>> {
        let reader = self
            .segment_reader
            .as_mut()
            .ok_or_else(|| Error::InvalidArgument("uninitialized".to_string()))?;

        reader.next().await.ok_or_else(||
                // Shouldn't reach the end of a stream.
                Error::NotFound(format!("stream {}", self.master_stream.stream_id())))
    }
}
