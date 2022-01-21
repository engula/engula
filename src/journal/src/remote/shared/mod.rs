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

use super::leader_based::{EpochState, Journal as LeaderBasedJournal, Role};

mod journal;
mod master;
mod orchestrator;
pub mod proto;
mod segment;
mod server;
mod stream_reader;
mod stream_writer;

pub use stream_reader::Reader as StreamReader;
pub use stream_writer::Writer as StreamWriter;

/// `Entry` is the minimum unit of the journal system. A continuous entries
/// compound a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
enum Entry {
    /// A placeholder, used in recovery phase.
    Hole,
    Event {
        epoch: u32,
        event: Box<[u8]>,
    },
    /// A bridge record, which identify the end of a segment.
    Bridge {
        epoch: u32,
    },
}

/// `SegmentMeta` records the metadata for locating a segment and its data.
#[derive(Debug)]
#[allow(dead_code)]
struct SegmentMeta {
    stream_name: String,

    /// A monotonic value in a stream. Allowing each segment's epoch value to be
    /// unique, it's easier to find a segment by its segment name and epoch.
    epoch: u32,

    /// Which journal server holds the segment's replica.
    copy_set: Vec<String>,
}

#[derive(Debug)]
#[allow(dead_code)]
enum ReplicaState {
    /// This replica isn't ready, it trying to copy entries from the other
    /// replicas.
    Placing,
    /// This replica is receiving entries.
    Receiving,
    /// This replica does not receive any entries.
    Sealed,
}

/// `ReplicaMeta` records the state of a replica and which segment it belongs
/// to.
#[derive(Debug)]
#[allow(dead_code)]
struct ReplicaMeta {
    stream_name: String,
    epoch: u32,
    state: ReplicaState,
}
