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

use super::{async_trait, error::Result, ResultStream};

// TODO: make it generic
pub type Timestamp = u64;

#[derive(Clone, Debug, Default)]
pub struct Event {
    pub ts: Timestamp,
    pub data: Vec<u8>,
}

/// An interface to manipulate events in a stream.
#[async_trait]
pub trait EventStream {
    /// Reads events since a timestamp (inclusive).
    async fn read_events(&self, ts: Timestamp) -> ResultStream<Event>;

    /// Appends an event with a timestamp.
    async fn append_event(&self, ts: Timestamp, data: Vec<u8>) -> Result<()>;

    /// Releases events up to a timestamp (exclusive).
    async fn release_events(&self, ts: Timestamp) -> Result<()>;
}
