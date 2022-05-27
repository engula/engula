use std::sync::Arc;

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

pub mod group_engine;
mod replica;
mod route_table;
pub mod state_engine;

use self::{
    replica::Replica,
    route_table::{RaftRouteTable, ReplicaRouteTable},
    state_engine::StateEngine,
};
use crate::Result;

#[allow(unused)]
#[derive(Clone)]
pub struct Node
where
    Self: Send + Sync,
{
    raw_db: Arc<rocksdb::DB>,
    state_engine: StateEngine,
    raft_route_table: RaftRouteTable,
    replica_route_table: ReplicaRouteTable,
}

#[allow(unused)]
impl Node {
    pub fn new(raw_db: Arc<rocksdb::DB>, state_engine: StateEngine) -> Self {
        Node {
            raw_db,
            state_engine,
            raft_route_table: RaftRouteTable::new(),
            replica_route_table: ReplicaRouteTable::new(),
        }
    }

    #[allow(unused)]
    pub async fn recover(&self) -> Result<()> {
        for (group_id, replica_id, state) in self.state_engine.iterate_replica_states().await {
            // TODO(walter)
        }
        todo!()
    }

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

    #[inline]
    pub fn state_engine(&self) -> &StateEngine {
        &self.state_engine
    }
}
