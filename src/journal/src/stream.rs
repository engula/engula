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

use crate::{async_trait, Result};

/// An increasing number to order events.
pub type Sequence = u64;

#[async_trait]
pub trait StreamReader {
    /// Seeks to the given sequence.
    async fn seek(&mut self, sequence: Sequence) -> Result<()>;

    /// Returns the next event.
    async fn try_next(&mut self) -> Result<Option<Vec<u8>>>;

    /// Returns the next event or waits until it is available.
    async fn wait_next(&mut self) -> Result<Vec<u8>>;
}

#[async_trait]
pub trait StreamWriter {
    /// Appends an event, returns the sequence of the event just append.
    async fn append(&mut self, event: Vec<u8>) -> Result<Sequence>;

    /// Truncates events up to a sequence (exclusive).
    async fn truncate(&mut self, sequence: Sequence) -> Result<()>;
}
