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

//! A stream of a journal is divided into multiple segments which distributed
//! in journal servers. A segment has multiple replicas which distributed in
//! journal servers.
//!
//! The sequence of entries within a segment is continuous, but it is not
//! guaranteed across segments.
//!
//! Entry sequence = (epoch << 32) | index of entry.
use async_trait::async_trait;

use super::Entry;
use crate::Result;

#[async_trait]
pub(super) trait SegmentReader {
    /// Seeks to the given index in current segment.
    async fn seek(&mut self, index: u32) -> Result<()>;

    /// Returns the next entry.
    async fn try_next(&mut self) -> Result<Option<Entry>>;

    /// Returns the next entry or waits until it is available.
    async fn watch_next(&mut self) -> Result<Entry>;
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct WriteRequest {
    /// The epoch of write request initiator, it's not always equal to segment's
    /// epoch.
    epoch: u32,
    /// The first index of entries.
    index: u32,
    /// The index of acked entries which:
    ///  1. the number of replicas is satisfied replication policy.
    ///  2. all previous entries are acked.
    acked: u32,
    entries: Vec<Entry>,
}

#[async_trait]
pub(super) trait SegmentWriter {
    /// Seal the `store` operations of current segment, and any write operations
    /// issued with a small epoch will be rejected.
    async fn seal(&self, new_epoch: u32) -> Result<()>;

    /// Store continuously entries with assigned index.
    async fn store(&self, request: WriteRequest) -> Result<()>;
}
