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

use async_trait::async_trait;
use engula_futures::stream::batch::ResultStream;

use super::Role;
use crate::{Error, Result, Sequence};

// The shared journal is divided by epoch, which might distributed in different
// node in different copy set. `EpochMeta` records the metadata of each epoch.
#[derive(Debug)]
pub struct EpochMeta {
    pub stream_name: String,
    pub epoch: u64,
    pub lsn_range: std::ops::Range<u64>,
    pub copy_set: Vec<String>,
}

pub const INITIAL_EPOCH: u64 = 0;

#[allow(dead_code)]
pub const INFINITY_EPOCH: u64 = u64::MAX;

/// The phase a member belongs to:
///
/// Catching -> Following -> Sealing -> Recovering -> Leading
///                 ^                                    |
///                 +------------------------------------+
#[derive(Debug)]
#[allow(dead_code)]
pub enum Phase {
    /// A member must catch the event lags before starting to follow a stream.
    Catching,
    /// A member must seals the former epochs before starting to recovery a
    /// stream.
    Sealing,
    /// A member must recovery all unfinished replications before starting to
    /// lead a stream.
    Recovering,
    /// A member is prepared to receive incoming events.
    Leading,
    /// A member is prepared to follow and subscribe a stream.
    Following,
}

/// The state of a member of a stream.
#[derive(Debug)]
pub struct MemberState {
    /// Member id.
    pub id: u64,
    /// The local epoch that the member is in.
    pub epoch: u64,
    pub phase: Phase,

    /// The sequence this member has already known committed.
    pub committed: Sequence,
    /// The sequence this member has already consumed.
    pub consumed: Sequence,
}

impl Default for MemberState {
    fn default() -> Self {
        MemberState {
            id: 0,
            epoch: INITIAL_EPOCH,
            phase: Phase::Catching,
            committed: 0,
            consumed: 0,
        }
    }
}

/// The commands of a master must be completed by a stream member.
#[derive(Debug)]
#[allow(dead_code)]
pub enum Command {
    /// Promote the epoch and specify the new role.
    Promote {
        role: Role,
        epoch: u64,
        leader: String,
    },
}

/// A abstraction of master of shared journal.
#[async_trait]
pub trait Master {
    type EpochMetaStream: ResultStream<Elem = EpochMeta, Error = Error>;

    /// Sends the state of a stream to master, and receives commands.
    async fn heartbeat(&self, stream_name: &str, state: &MemberState) -> Result<Vec<Command>>;

    /// Query the meta of epochs covered by the specified range.
    async fn query_epoch_meta(
        &self,
        stream_name: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Self::EpochMetaStream>;

    /// Get epoch meta of the specified epoch of a stream.
    async fn get_epoch_meta(&self, stream_name: &str, epoch: u64) -> Result<EpochMeta>;
}
