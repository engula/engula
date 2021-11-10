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

pub type SequenceNumber = u64;

#[derive(Clone, Debug, Default)]
pub struct JournalRecord {
    pub sn: SequenceNumber,
    pub data: Vec<u8>,
}

/// An interface to manipulate records of a journal stream.
#[async_trait]
pub trait JournalStream {
    /// Reads records since a sequence number (inclusive).
    async fn read_records(&self, sn: SequenceNumber) -> ResultStream<JournalRecord>;

    /// Appends a record with a sequence number.
    async fn append_record(&self, sn: SequenceNumber, data: Vec<u8>) -> Result<()>;

    /// Releases records up to a sequence number (exclusive).
    async fn release_records(&self, sn: SequenceNumber) -> Result<()>;
}
