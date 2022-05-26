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

use super::Replica;

/// A structure support replica route queries.
#[allow(unused)]
pub struct ReplicaRouteTable
where
    Self: Send + Sync, {}

// FIXME(walter) define.
#[allow(unused)]
pub struct RootReplica {}

#[allow(unused)]
impl ReplicaRouteTable {
    pub fn find(&self, replica_id: u64) -> Option<&Replica> {
        todo!()
    }

    pub fn find_root(&self) -> Option<&RootReplica> {
        todo!()
    }

    pub fn upsert(&self, replica: &Replica) {
        todo!()
    }

    pub fn upsert_root(&self, root_replica: &RootReplica) {
        todo!()
    }

    pub fn remove(&self, replica_id: u64) {
        todo!()
    }
}

/// A structure support raft route table query.
#[allow(unused)]
pub struct RaftRouteTable
where
    Self: Send + Sync, {}

#[allow(unused)]
impl RaftRouteTable {
    pub fn find(&self, replica_id: u64) -> Option<()> {
        todo!("define raft sender")
    }

    pub fn upsert(&self, replica_id: u64, sender: ()) -> Option<()> {
        todo!()
    }

    pub fn delete(&self, replica_id: u64) {
        todo!()
    }
}
