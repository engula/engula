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

use std::convert::TryInto;

use crate::{async_trait, Error, Result, ResultStream};

/// A generic timestamp to order events.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn serialize(&self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }

    pub fn deserialize(bytes: Vec<u8>) -> Result<Self> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|v| Error::Unknown(format!("malformed bytes: {:?}", v)))?;
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

impl From<u64> for Timestamp {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub ts: Timestamp,
    pub data: Vec<u8>,
}

/// An interface to manipulate a stream.
#[async_trait]
pub trait Stream: Clone + Send + Sync + 'static {
    /// Reads events since a timestamp (inclusive).
    async fn read_events(&self, ts: Timestamp) -> ResultStream<Vec<Event>>;

    /// Appends an event.
    async fn append_event(&self, event: Event) -> Result<()>;

    /// Releases events up to a timestamp (exclusive).
    async fn release_events(&self, ts: Timestamp) -> Result<()>;
}
