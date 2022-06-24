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

use std::sync::Arc;

use engula_api::server::v1::{group_request_union::Request, group_response_union::Response};

use super::ForwardCtx;
use crate::{
    node::Replica, raftgroup::AddressResolver, runtime::Executor, serverpb::v1::*, Result,
};

#[derive(Clone)]
pub struct MigrateController {
    shared: Arc<MigrateControllerShared>,
}

struct MigrateControllerShared {
    executor: Executor,
    address_resolver: Arc<dyn AddressResolver>,
}

impl MigrateController {
    pub fn new(address_resolver: Arc<dyn AddressResolver>, executor: Executor) -> Self {
        MigrateController {
            shared: Arc::new(MigrateControllerShared {
                address_resolver,
                executor,
            }),
        }
    }

    pub fn migrate(&self, replica: Arc<Replica>, migrate_meta: MigrateMeta) {
        match MigrateState::from_i32(migrate_meta.state).unwrap() {
            MigrateState::Initial => {
                // Send begin migration request to source group.
            }
            MigrateState::Migrating => {
                // pull shard chunk from source group.
                self.pull(replica, migrate_meta);
            }
            MigrateState::HalfFinished => {
                // Send finish migration request to source group.
            }
            MigrateState::Finished | MigrateState::Aborted => {
                // Already finished.
            }
        }
    }

    pub async fn forward(&self, forward_ctx: ForwardCtx, request: &Request) -> Result<Response> {
        super::forward_request(self.shared.address_resolver.clone(), &forward_ctx, request).await
    }

    fn pull(&self, replica: Arc<Replica>, migrate_meta: MigrateMeta) {
        use crate::runtime::TaskPriority;

        let replica_info = replica.replica_info();
        let tag_owner = replica_info.group_id.to_le_bytes();
        let tag = Some(tag_owner.as_slice());
        let address_resolver = self.shared.address_resolver.clone();
        self.shared
            .executor
            .spawn(tag, TaskPriority::IoHigh, async move {
                super::pull_shard(address_resolver, replica, migrate_meta).await;
            });
    }
}
