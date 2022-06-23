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

#![allow(clippy::all)]

pub mod v1 {
    use engula_api::server::v1::ShardDesc;

    tonic::include_proto!("serverpb.v1");

    pub type ApplyState = EntryId;

    impl SyncOp {
        pub fn add_shard(shard: ShardDesc) -> Self {
            SyncOp {
                add_shard: Some(AddShard { shard: Some(shard) }),
                ..Default::default()
            }
        }

        pub fn purge_replica(orphan_replica_id: u64) -> Self {
            SyncOp {
                purge_replica: Some(PurgeOrphanReplica {
                    replica_id: orphan_replica_id,
                }),
                ..Default::default()
            }
        }

        pub fn migrate_event(value: migrate_event::Value) -> Self {
            SyncOp {
                migrate_event: Some(MigrateEvent { value: Some(value) }),
                ..Default::default()
            }
        }
    }
}
