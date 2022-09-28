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

use engula_api::server::v1::{group_request_union::Request, group_response_union::Response, *};
use engula_client::{MigrateClient, Router};
use futures::{channel::mpsc, StreamExt};
use tracing::{debug, error, info, warn};

use crate::{
    node::Replica, runtime::sync::WaitGroup, serverpb::v1::*, NodeConfig, Provider, Result,
};

#[derive(Debug)]
pub struct ForwardCtx {
    pub shard_id: u64,
    pub dest_group_id: u64,
    pub payloads: Vec<ShardData>,
}

struct MigrationCoordinator {
    cfg: NodeConfig,

    replica_id: u64,
    group_id: u64,

    replica: Arc<Replica>,

    client: MigrateClient,
    desc: MigrationDesc,
}

#[derive(Clone)]
pub struct MigrateController {
    shared: Arc<MigrateControllerShared>,
}

struct MigrateControllerShared {
    cfg: NodeConfig,
    provider: Arc<Provider>,
}

impl MigrateController {
    pub(crate) fn new(cfg: NodeConfig, provider: Arc<Provider>) -> Self {
        MigrateController {
            shared: Arc::new(MigrateControllerShared { cfg, provider }),
        }
    }

    pub fn router(&self) -> Router {
        self.shared.provider.router.clone()
    }

    /// Watch migration state and do the corresponding step.
    pub async fn watch_state_changes(
        &self,
        replica: Arc<Replica>,
        mut receiver: mpsc::UnboundedReceiver<MigrationState>,
        wait_group: WaitGroup,
    ) {
        use crate::runtime::{current, TaskPriority};

        let info = replica.replica_info();
        let replica_id = info.replica_id;
        let group_id = info.group_id;

        let ctrl = self.clone();
        current()
            .spawn(Some(group_id), TaskPriority::High, async move {
                let mut coord: Option<MigrationCoordinator> = None;
                while let Some(state) = receiver.next().await {
                    debug!(
                        replica = replica_id,
                        group = group_id,
                        "on migration step: {:?}",
                        MigrationStep::from_i32(state.step)
                    );
                    let desc = state.get_migration_desc();
                    if coord.is_none() || coord.as_ref().unwrap().desc != *desc {
                        let target_group_id = if desc.src_group_id == group_id {
                            desc.dest_group_id
                        } else {
                            desc.src_group_id
                        };
                        let client = MigrateClient::new(
                            target_group_id,
                            ctrl.shared.provider.router.clone(),
                            ctrl.shared.provider.conn_manager.clone(),
                        );
                        coord = Some(MigrationCoordinator {
                            cfg: ctrl.shared.cfg.clone(),
                            replica_id,
                            group_id,
                            replica: replica.clone(),
                            client,
                            desc: desc.clone(),
                        });
                    }
                    coord.as_mut().unwrap().next_step(state).await;
                }
                debug!(
                    replica = replica_id,
                    group = group_id,
                    "migration state watcher is stopped",
                );
                drop(wait_group);
            })
            .await;
    }

    pub async fn forward(&self, forward_ctx: ForwardCtx, request: &Request) -> Result<Response> {
        let group_id = forward_ctx.dest_group_id;
        let mut client = MigrateClient::new(
            group_id,
            self.shared.provider.router.clone(),
            self.shared.provider.conn_manager.clone(),
        );
        let req = ForwardRequest {
            shard_id: forward_ctx.shard_id,
            group_id,
            forward_data: forward_ctx.payloads.clone(),
            request: Some(GroupRequestUnion {
                request: Some(request.clone()),
            }),
        };
        let resp = client.forward(&req).await?;
        let resp = resp.response.and_then(|resp| resp.response);
        Ok(resp.unwrap())
    }
}

impl MigrationCoordinator {
    async fn next_step(&mut self, state: MigrationState) {
        let step = MigrationStep::from_i32(state.step).unwrap();
        if self.is_dest_group() {
            match step {
                MigrationStep::Prepare => {
                    self.setup_source_group().await;
                }
                MigrationStep::Migrating => {
                    self.pull(state.last_migrated_key).await;
                }
                MigrationStep::Migrated => {
                    // Send finish migration request to source group.
                    self.commit_source_group().await;
                }
                MigrationStep::Finished | MigrationStep::Aborted => unreachable!(),
            }
        } else {
            match step {
                MigrationStep::Migrated => {
                    self.clean_orphan_shard().await;
                }
                MigrationStep::Prepare | MigrationStep::Migrating => {}
                MigrationStep::Finished | MigrationStep::Aborted => unreachable!(),
            }
        }
    }

    async fn setup_source_group(&mut self) {
        debug!(
            replica = self.replica_id,
            group = self.group_id,
            desc = %self.desc,
            "setup source group migration"
        );

        match self.client.setup_migration(&self.desc).await {
            Ok(_) => {
                info!(replica = self.replica_id,
                    group = self.group_id,
                    desc = %self.desc,
                    "setup source group migration success"
                );
                self.enter_pulling_step().await;
            }
            Err(engula_client::Error::EpochNotMatch(group_desc)) => {
                // Since the epoch is not matched, this migration should be rollback.
                warn!(replica = self.replica_id, group = self.group_id, desc = %self.desc,
                    "abort migration since epoch not match, new epoch is {}",
                        group_desc.epoch);
                self.abort_migration().await;
            }
            Err(err) => {
                error!(replica = self.replica_id,
                    group = self.group_id,
                    desc= %self.desc,
                    "setup source group migration: {}", err);
            }
        }
    }

    async fn commit_source_group(&mut self) {
        if let Err(e) = self.client.commit_migration(&self.desc).await {
            error!(replica = self.replica_id,
                group = self.group_id,
                desc = %self.desc,
                "commit source group migration: {}", e);
            return;
        }

        info!(replica = self.replica_id,
            group = self.group_id,
            desc = %self.desc,
            "source group migration is committed");

        self.clean_migration_state().await;
    }

    async fn commit_dest_group(&self) {
        if let Err(e) = self.replica.commit_migration(&self.desc).await {
            error!(replica = self.replica_id,
                group = self.group_id,
                desc = %self.desc,
                "commit dest migration: {}", e);
            return;
        }

        info!(replica = self.replica_id,
            group = self.group_id,
            desc = %self.desc,
            "dest group migration is committed");
    }

    async fn clean_migration_state(&self) {
        if let Err(e) = self.replica.finish_migration(&self.desc).await {
            error!(
                replica = self.replica_id,
                group = self.group_id,
                desc = %self.desc,
                "clean migration state: {}", e);
            return;
        }

        info!(replica = self.replica_id,
            group = self.group_id,
            desc = %self.desc,
            "migration state is cleaned");
    }

    async fn abort_migration(&self) {
        if let Err(e) = self.replica.abort_migration(&self.desc).await {
            error!(
                replica = self.replica_id,
                group = self.group_id,
                desc = %self.desc,
                err = ?e,
                "abort migration",
            );
            return;
        }

        info!(replica = self.replica_id,
            group = self.group_id,
            desc = %self.desc,
            "migration is aborted");
    }

    async fn enter_pulling_step(&self) {
        if let Err(e) = self.replica.enter_pulling_step(&self.desc).await {
            error!(replica = self.replica_id,
                group = self.group_id,
                desc = %self.desc,
                "enter pulling step: {}", e);
        }
    }

    async fn clean_orphan_shard(&self) {
        use super::gc::remove_shard;

        let group_engine = self.replica.group_engine();
        if let Err(e) = remove_shard(
            &self.cfg,
            self.replica.as_ref(),
            group_engine,
            self.desc.get_shard_id(),
        )
        .await
        {
            error!(replica = self.replica_id,
                group = self.group_id,
                desc = %self.desc,
                "remove migrated shard from source group: {}", e);
            return;
        }

        self.clean_migration_state().await;
    }

    async fn pull(&mut self, last_migrated_key: Vec<u8>) {
        if let Err(e) = super::pull_shard(
            &mut self.client,
            self.replica.as_ref(),
            &self.desc,
            last_migrated_key,
        )
        .await
        {
            error!(replica = self.replica_id,
                group = self.group_id,
                desc = %self.desc,
                "pull shard from source group: {}", e);
            return;
        }

        self.commit_dest_group().await;
    }

    #[inline]
    fn is_dest_group(&self) -> bool {
        self.group_id == self.desc.dest_group_id
    }
}
