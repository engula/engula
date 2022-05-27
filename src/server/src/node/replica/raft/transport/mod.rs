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
use std::future::Future;

use engula_api::server::v1::NodeDesc;

use crate::{
    engula::server::v1::{RaftMessage, SnapshotRequest},
    Result,
};

/// A structures represent the underlying connection between two nodes. It only used by raft message
/// and snapshot.
#[allow(unused)]
pub struct Transport {}

/// A logic connection between two nodes. A [`Channel`] is bind to a specific target, the name
/// lookup are finished by internal machenism.
#[allow(unused)]
pub struct Channel {
    target_id: u64,
}

#[allow(unused)]
impl Channel {
    pub async fn send_message(msg: RaftMessage) -> Result<()> {
        todo!()
    }

    pub async fn retrive_snapshot(request: SnapshotRequest) -> Result<()> {
        todo!()
    }
}

/// An abstraction for resolving address by node id.
pub trait AddressResolver: Send + Sync {
    fn resolve(&self, node_id: u64) -> Box<dyn Future<Output = Result<NodeDesc>>>;
}

/// Manage transports. This structure is used by all groups.
///
/// A transport is recycled by manager, if it exceeds the idle intervals.
#[allow(unused)]
pub struct TransportManager
where
    Self: Send + Sync,
{
    resolver: Box<dyn AddressResolver>,
}

#[allow(unused)]
impl TransportManager {
    /// Make new channel to target.
    pub fn channel(&self, target_node_id: u64) -> Result<Channel> {
        todo!()
    }
}
