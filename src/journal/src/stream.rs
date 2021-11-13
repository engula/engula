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

use std::fmt::Debug;

use super::{async_trait, error::Result, ResultStream};

pub trait Timestamp: Ord + Debug + Send + Copy + Default + 'static {}

impl<T> Timestamp for T where T: Ord + Debug + Send + Copy + Default + 'static {}

#[derive(Clone, Debug, Default)]
pub struct Event<Ts: Timestamp> {
    pub ts: Ts,
    pub data: Vec<u8>,
}

/// An interface to manipulate events in a stream.
#[async_trait]
pub trait Stream<Ts: Timestamp> {
    /// Reads events since a timestamp (inclusive).
    async fn read_events(&self, ts: Ts) -> ResultStream<Event<Ts>>;

    /// Appends an event with a timestamp.
    async fn append_event(&self, ts: Ts, data: Vec<u8>) -> Result<()>;

    /// Releases events up to a timestamp (exclusive).
    async fn release_events(&self, ts: Ts) -> Result<()>;
}
