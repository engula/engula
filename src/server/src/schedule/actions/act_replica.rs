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

use engula_api::server::v1::*;
use tracing::{debug, info, warn};

use super::{Action, ActionState};
use crate::{root::RemoteStore, schedule::scheduler::ScheduleContext, Provider};

pub(crate) struct CreateReplicas {
    pub replicas: Vec<ReplicaDesc>,
}

pub(crate) struct RemoveReplica {
    pub group: GroupDesc,
    pub replica: ReplicaDesc,
}

pub(crate) struct ClearReplicaState {
    pub target_id: u64,
}

impl CreateReplicas {
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

        for r in &self.replicas {
            if let Err(e) = self
                .create_replica(group_id, r, ctx.provider.as_ref())
                .await
            {
                warn!("group {group_id} replica {replica_id} task {task_id} create replicas {r:?}: {e}");
                return ActionState::Pending(None);
            }
        }

        info!("group {group_id} replica {replica_id} task {task_id} create replicas success");
        ActionState::Done
    }

    async fn poll(&mut self, _task_id: u64, _ctx: &mut ScheduleContext<'_>) -> ActionState {
        ActionState::Done
    }
}

impl RemoveReplica {
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
                info!(
                    "group {group_id} replica {replica_id} task {task_id} remove replica success"
                );
                ActionState::Done
            }
            Err(engula_client::Error::Rpc(status))
                if engula_client::error::retryable_rpc_err(&status) =>
            {
                debug!("group {group_id} replica {replica_id} task {task_id} remove replica {replica:?}: {status}");
                ActionState::Pending(None)
            }
            Err(e) => {
                warn!(
                    "group {group_id} replica {replica_id} task {task_id} remove replica {replica:?}: {e}"
                );
                ActionState::Pending(None)
            }
        }
    }

    async fn poll(&mut self, _task_id: u64, _ctx: &mut ScheduleContext<'_>) -> ActionState {
        ActionState::Done
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
            warn!("group {group_id} replica {replica_id} task {task_id} clear replica state of {target_id}: {e}");
            return ActionState::Pending(None);
        }

        info!("group {group_id} replica {replica_id} task {task_id} clear replica state success");

        ActionState::Done
    }

    async fn poll(&mut self, _task_id: u64, _ctx: &mut ScheduleContext<'_>) -> ActionState {
        ActionState::Done
    }
}
