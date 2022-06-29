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

use std::{future::Future, sync::Arc};

use engula_api::server::v1::{group_request_union::Request, group_response_union::Response, *};
use futures::{channel::mpsc, StreamExt};
use tracing::{debug, error, info, warn};

use super::{ForwardCtx, GroupClient};
use crate::{
    node::{replica::MigrateAction, Replica},
    raftgroup::AddressResolver,
    runtime::Executor,
    serverpb::v1::*,
    Error, Result,
};

#[derive(Clone)]
pub struct MigrateController {
    shared: Arc<MigrateControllerShared>,
}

struct MigrateControllerShared {
    executor: Executor,
    address_resolver: Arc<dyn AddressResolver>,
}

#[allow(unused)]
impl MigrateController {
    pub fn new(address_resolver: Arc<dyn AddressResolver>, executor: Executor) -> Self {
        MigrateController {
            shared: Arc::new(MigrateControllerShared {
                address_resolver,
                executor,
            }),
        }
    }

    /// Watch migration state and do the corresponding step.
    pub fn watch_state_changes(
        &self,
        replica: Arc<Replica>,
        mut receiver: mpsc::UnboundedReceiver<MigrationState>,
    ) {
        let info = replica.replica_info();
        let replica_id = info.replica_id;
        let group_id = info.group_id;

        let ctrl = self.clone();
        self.spawn_group_task(group_id, async move {
            while let Some(state) = receiver.next().await {
                debug!(
                    "replica {} group {} step migration step {:?}",
                    replica_id,
                    group_id,
                    MigrationStep::from_i32(state.step)
                );
                ctrl.on_migration_step(group_id, &replica, state).await;
            }
            debug!("replica {} migration state watcher is stopped", replica_id);
        });
    }

    async fn on_migration_step(&self, group_id: u64, replica: &Replica, state: MigrationState) {
        let info = replica.replica_info();
        if is_migration_dest_group(&state, group_id) {
            self.on_dest_group_step(replica, state).await;
        } else {
            self.on_src_group_step(replica, state).await;
        }
    }

    // TODO(walter) call this once migration has committed!.
    async fn on_src_group_step(&self, replica: &Replica, state: MigrationState) {
        let desc = state
            .migration_desc
            .expect("MigrationState::migration_desc is not None");
        match MigrationStep::from_i32(state.step).unwrap() {
            MigrationStep::Prepare => {}
            MigrationStep::Migrating => {}
            MigrationStep::Migrated => {
                self.clean_orphan_shard(replica, desc);
            }
        }
    }

    async fn on_dest_group_step(&self, replica: &Replica, state: MigrationState) {
        let desc = state
            .migration_desc
            .expect("MigrationState::migration_desc is not None");
        debug!(desc = ?desc, "on dest group step");
        match MigrationStep::from_i32(state.step).unwrap() {
            MigrationStep::Prepare => {
                self.initialize_src_group(replica, desc).await;
            }
            MigrationStep::Migrating => {
                // pull shard chunk from source group.
                self.pull(replica, desc, state.last_migrated_key).await;
            }
            MigrationStep::Migrated => {
                // Send finish migration request to source group.
                self.commit_source_group(replica, desc).await;
            }
        }
    }

    async fn initialize_src_group(&self, replica: &Replica, desc: MigrationDesc) {
        let shard_desc = desc
            .shard_desc
            .clone()
            .expect("MigrationDesc::shard_desc is not None");

        let shard_id = shard_desc.id;
        let expect_epoch = Some(desc.src_group_epoch);
        let mut group_client = GroupClient::new(
            desc.src_group_id,
            expect_epoch,
            self.shared.address_resolver.clone(),
        );

        let req = MigrateRequest {
            desc: Some(desc.clone()),
            action: migrate_request::Action::Prepare as i32,
        };
        match group_client.migrate(req).await {
            Ok(_) => {
                info!("initial source group success, to migrating step");
                self.enter_pulling_step(replica, desc).await;
            }
            Err(Error::EpochNotMatch(_)) => {
                // Since the epoch is not matched, this migration should be rollback.
                warn!(
                    "abort migration of shard {:?} since epoch not match",
                    shard_desc
                );
                self.abort_migration(replica, desc).await;
            }
            Err(err) => {
                error!("initial source group: {}", err);
            }
        }
    }

    async fn commit_source_group(&self, replica: &Replica, desc: MigrationDesc) {
        use super::GroupClient;

        let shard_desc = desc.shard_desc.clone().unwrap();
        let shard_id = shard_desc.id;
        let mut group_client = GroupClient::new(
            desc.src_group_id,
            None,
            self.shared.address_resolver.clone(),
        );

        let req = MigrateRequest {
            desc: Some(desc.clone()),
            action: migrate_request::Action::Commit as i32,
        };
        match group_client.migrate(req).await {
            Err(err) => {
                error!("commit source group: {}", err);
            }
            Ok(_) => {
                info!("commit source group success, try clean migration states");
                self.clean_migration_state(replica, desc).await;
            }
        }
    }

    async fn commit_dest_group(&self, replica: &Replica, desc: MigrationDesc) {
        let shard_desc = desc.shard_desc.clone().unwrap();
        let shard_id = shard_desc.id;
        match replica.migrate(&desc, MigrateAction::Commit).await {
            Ok(()) => {
                info!("commit migration success");
            }
            Err(err) => {
                error!("commit migration of shard {}: {}", shard_id, err);
            }
        }
    }

    async fn clean_migration_state(&self, replica: &Replica, desc: MigrationDesc) {
        let shard_desc = desc.shard_desc.clone().unwrap();
        let shard_id = shard_desc.id;
        match replica.migrate(&desc, MigrateAction::Clean).await {
            Ok(()) => {
                // migration is finished
            }
            Err(err) => {
                error!("clean migration state of shard {}: {}", shard_id, err);
            }
        }
    }

    async fn abort_migration(&self, replica: &Replica, desc: MigrationDesc) {
        let shard_desc = desc.shard_desc.clone().unwrap();
        let shard_id = shard_desc.id;
        match replica.migrate(&desc, MigrateAction::Abort).await {
            Ok(()) => {}
            Err(err) => {
                error!("abort migration of shard {}: {}", shard_id, err);
            }
        }
    }

    async fn enter_pulling_step(&self, replica: &Replica, desc: MigrationDesc) {
        let shard_desc = desc.shard_desc.clone().unwrap();
        let shard_id = shard_desc.id;
        match replica.migrate(&desc, MigrateAction::Migrating).await {
            Ok(()) => {
                info!("enter pulling step success");
            }
            Err(err) => {
                error!("pulling migration of shard {}: {}", shard_id, err);
            }
        }
    }

    async fn clean_orphan_shard(&self, replica: &Replica, desc: MigrationDesc) {
        use super::gc::remove_shard;

        let shard_desc = desc.shard_desc.clone().unwrap();
        let shard_id = shard_desc.id;
        match remove_shard(replica, replica.group_engine(), shard_id).await {
            Ok(()) => {
                info!("remove orphan shard success, try clean migration states");
                self.clean_migration_state(replica, desc).await;
            }
            Err(err) => {
                error!("clean orphan shard: {}", err);
            }
        }
    }

    pub async fn forward(&self, forward_ctx: ForwardCtx, request: &Request) -> Result<Response> {
        super::forward_request(self.shared.address_resolver.clone(), &forward_ctx, request).await
    }

    async fn pull(&self, replica: &Replica, desc: MigrationDesc, last_migrated_key: Vec<u8>) {
        let info = replica.replica_info();
        let shard_id = desc.shard_desc.as_ref().unwrap().id;
        let mut group_client = GroupClient::new(
            desc.src_group_id,
            None,
            self.shared.address_resolver.clone(),
        );

        match super::pull_shard(&mut group_client, replica, &desc, last_migrated_key).await {
            Ok(()) => {
                info!("pull shard success");
                self.commit_dest_group(replica, desc).await;
            }
            Err(err) => {
                error!(
                    "replica {} pull shard {}: {}",
                    info.replica_id, shard_id, err
                );
            }
        }
    }

    fn spawn_group_task<F, T>(&self, group_id: u64, future: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        use crate::runtime::TaskPriority;

        let tag_owner = group_id.to_le_bytes();
        let tag = Some(tag_owner.as_slice());
        self.shared
            .executor
            .spawn(tag, TaskPriority::IoHigh, future);
    }
}

#[inline]
fn is_migration_dest_group(state: &MigrationState, group_id: u64) -> bool {
    state
        .migration_desc
        .as_ref()
        .map(|d| d.dest_group_id == group_id)
        .unwrap_or_default()
}
