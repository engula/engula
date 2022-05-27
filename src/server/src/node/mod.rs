use engula_api::server::v1::GroupDesc;

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

mod group_engine;
mod replica;
mod route_table;
mod state_engine;

use self::{
    replica::Replica,
    route_table::{RaftRouteTable, ReplicaRouteTable},
};
use crate::Result;

#[allow(unused)]
pub struct Node
where
    Self: Send + Sync,
{
    raft_route_table: RaftRouteTable,
    replica_route_table: ReplicaRouteTable,
}

#[allow(unused)]
impl Node {
    /// Create a replica.
    ///
    /// The replica state is determined by the `GroupDesc`.
    ///
    /// NOTE: This function is idempotent.
    pub async fn create_replica(&self, replica_id: u64, group: GroupDesc) -> Result<()> {
        todo!()
    }

    /// Terminate specified replica.
    pub async fn terminate_replica(&self, replica_id: u64) -> Result<()> {
        todo!()
    }

    #[inline]
    pub fn replica_table(&self) -> &ReplicaRouteTable {
        &self.replica_route_table
    }

    #[inline]
    pub fn raft_route_table(&self) -> &RaftRouteTable {
        &self.raft_route_table
    }
}
