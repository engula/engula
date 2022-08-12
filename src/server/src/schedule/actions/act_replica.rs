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

use std::time::Duration;

use engula_api::server::v1::*;
use tracing::{debug, info, warn};

use super::{Action, ActionState};
use crate::{root::RemoteStore, schedule::scheduler::ScheduleContext, Provider};

pub(crate) struct CreateReplicas {
    pub replicas: Vec<ReplicaDesc>,
    interval_ms: u64,
    retry_count: usize,
}

pub(crate) struct RemoveReplica {
    pub group: GroupDesc,
    pub replica: ReplicaDesc,
    retry_count: usize,
}

pub(crate) struct ClearReplicaState {
    pub target_id: u64,
}

impl CreateReplicas {
    pub fn new(replicas: Vec<ReplicaDesc>) -> Self {
        CreateReplicas {
            replicas,
            interval_ms: 50,
            retry_count: 0,
        }
    }

    async fn create_replica(
        &self,
        group_id: u64,
        r: &ReplicaDesc,
        provider: &Provider,
    ) -> std::result::Result<(), engula_client::Error> {
        let addr = provider.router.find_node_addr(r.node_id)?;
        let client = provider.conn_manager.get_node_client(addr).await?;
        let desc = GroupDesc {
            id: group_id,
            ..Default::default()
        };
        client.create_replica(r.id, desc).await?;
        Ok(())
    }
}

#[crate::async_trait]
impl Action for CreateReplicas {
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;

        while let Some(r) = self.replicas.last() {
            match self
                .create_replica(group_id, r, ctx.provider.as_ref())
                .await
            {
                Ok(()) => {
                    self.retry_count = 0;
                    self.interval_ms = 50;
                    self.replicas.pop();
                }
                Err(engula_client::Error::Rpc(status))
                    if engula_client::error::retryable_rpc_err(&status)
                        && self.retry_count < 30 =>
                {
                    debug!("group {group_id} replica {replica_id} task {task_id} create replica {r:?}: {status}");
                    self.retry_count += 1;
                    self.interval_ms = std::cmp::min(self.interval_ms * 2, 1000);
                    return ActionState::Pending(Some(Duration::from_millis(self.interval_ms)));
                }
                Err(e) => {
                    warn!("group {group_id} replica {replica_id} task {task_id} abort creating replica {r:?}: {e}");
                    return ActionState::Aborted;
                }
            }
        }

        info!("group {group_id} replica {replica_id} task {task_id} create replicas success");
        ActionState::Done
    }
}

impl RemoveReplica {
    pub fn new(group: GroupDesc, replica: ReplicaDesc) -> Self {
        RemoveReplica {
            group,
            replica,
            retry_count: 0,
        }
    }

    async fn remove_replica(
        &self,
        r: &ReplicaDesc,
        group: GroupDesc,
        provider: &Provider,
    ) -> std::result::Result<(), engula_client::Error> {
        let addr = provider.router.find_node_addr(r.node_id)?;
        let client = provider.conn_manager.get_node_client(addr).await?;
        client.remove_replica(r.id, group.clone()).await?;
        Ok(())
    }
}

#[crate::async_trait]
impl Action for RemoveReplica {
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;
        let replica = &self.replica;
        match self
            .remove_replica(replica, self.group.clone(), ctx.provider.as_ref())
            .await
        {
            Ok(()) => {
                info!("group {group_id} replica {replica_id} task {task_id} remove replica {replica:?} success");
                return ActionState::Done;
            }
            Err(engula_client::Error::Rpc(status))
                if engula_client::error::retryable_rpc_err(&status) && self.retry_count < 3 =>
            {
                debug!("group {group_id} replica {replica_id} task {task_id} remove replica {replica:?}: {status}");
                self.retry_count += 1;
                ActionState::Pending(Some(Duration::from_secs(30)))
            }
            Err(e) => {
                warn!("group {group_id} replica {replica_id} task {task_id} abort removing replica {replica:?}: {e}");
                ActionState::Aborted
            }
        }
    }
}

impl ClearReplicaState {
    pub fn new(target_id: u64) -> Self {
        ClearReplicaState { target_id }
    }
}

#[crate::async_trait]
impl Action for ClearReplicaState {
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        let group_id = ctx.group_id;
        let replica_id = ctx.replica_id;
        let target_id = self.target_id;

        let root_store = RemoteStore::new(ctx.provider.clone());
        if let Err(e) = root_store.clear_replica_state(group_id, target_id).await {
            warn!("group {group_id} replica {replica_id} task {task_id} abort clearing replica {target_id} state: {e}");
            ActionState::Aborted
        } else {
            info!("group {group_id} replica {replica_id} task {task_id} clear replica {target_id} state success");
            ActionState::Done
        }
    }
}
