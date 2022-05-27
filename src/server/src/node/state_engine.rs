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

use engula_api::server::v1::NodeDesc;

use crate::Result;

/// FIXME(walter) define in proto files?
#[allow(unused)]
#[derive(Debug)]
pub enum ReplicaState {
    /// With membership, but couldn't supply service.  It is used in group creation.
    Initial,
    /// Without membership, only response raft messages.
    Pending,
    Normal,
    /// The service and memory states are shutdown and cleans, but disk data still exists.
    Terminated,
    Tombstone,
}

/// A structure supports saving and loading local states.
///
/// Local states:
/// - node ident
/// - root node descriptors
/// - replica states
///
/// NOTE: The group descriptors is stored in the corresponding GroupEngine, which is to ensure
/// that both the changes of group descriptor and data are persisted to disk in atomic.
#[allow(unused)]
pub struct StateEngine
where
    Self: Send + Sync, {}

#[allow(unused)]
pub struct ReplicaStateIterator {}

#[allow(unused)]
impl StateEngine {
    /// Read node ident from engine. `None` is returned if no such ident exists.
    ///
    /// TODO(walter) define NodeIdent.
    pub async fn read_ident(&self) -> Result<Option<()>> {
        todo!()
    }

    /// Save node ident, return appropriate error if ident already exists.
    ///
    /// TODO(walter) define NodeIdent
    pub async fn save_ident(&self, ident: ()) -> Result<()> {
        todo!()
    }

    /// Save root nodes.
    pub async fn save_root_nodes(&self, nodes: Vec<NodeDesc>) -> Result<()> {
        todo!()
    }

    /// Load root nodes. `None` is returned if there no any root node records exists.
    pub async fn load_root_nodes(&self) -> Result<Option<Vec<NodeDesc>>> {
        todo!()
    }

    /// Save replica state.
    pub async fn save_replica_state(
        &self,
        group_id: u64,
        replica_id: u64,
        state: ReplicaState,
    ) -> Result<()> {
        todo!()
    }

    /// Iterate group states.
    pub async fn iterate_replica_states(&self) -> ReplicaStateIterator {
        todo!()
    }
}

impl Iterator for ReplicaStateIterator {
    /// (group id, replica id, replica state)
    type Item = (u64, u64, ReplicaState);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
