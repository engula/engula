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

use serde::{de::DeserializeOwned, Serialize};

use crate::{async_trait, Result, ResultStream};

/// A generic timestamp to order events.
pub trait Timestamp:
    Ord + Send + Sync + Copy + Debug + Unpin + Serialize + DeserializeOwned + 'static
{
}

impl<T: Ord + Send + Sync + Copy + Debug + Unpin + Serialize + DeserializeOwned + 'static> Timestamp
    for T
{
}

#[derive(Clone, Debug, PartialEq)]
pub struct Event<T: Timestamp> {
    pub ts: T,
    pub data: Vec<u8>,
}

/// An interface to manipulate a stream.
#[async_trait]
pub trait Stream<T: Timestamp> {
    /// Reads events since a timestamp (inclusive).
    async fn read_events(&self, ts: T) -> ResultStream<Vec<Event<T>>>;

    /// Appends an event.
    async fn append_event(&self, event: Event<T>) -> Result<()>;

    /// Releases events up to a timestamp (exclusive).
    async fn release_events(&self, ts: T) -> Result<()>;
}
