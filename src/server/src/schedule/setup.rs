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

use tracing::debug;

use crate::{
    bootstrap::ROOT_GROUP_ID,
    node::{replica::ReplicaConfig, Replica},
    runtime::{sync::WaitGroup, TaskPriority},
    schedule::{
        event_source::EventSource, provider::GroupProviders, scheduler::Scheduler, task::Task,
    },
    Provider,
};

pub(crate) fn setup_scheduler(
    cfg: ReplicaConfig,
    provider: Arc<Provider>,
    replica: Arc<Replica>,
    wait_group: WaitGroup,
) {
    let group_providers = Arc::new(GroupProviders::new(
        replica.clone(),
        provider.router.clone(),
    ));

    let group_id = replica.replica_info().group_id;
    let tag = &group_id.to_le_bytes();
    let executor = provider.executor.clone();
    executor.spawn(Some(tag), TaskPriority::Low, async move {
        scheduler_main(cfg, replica, provider, group_providers).await;
        drop(wait_group);
    });
}

async fn scheduler_main(
    cfg: ReplicaConfig,
    replica: Arc<Replica>,
    provider: Arc<Provider>,
    group_providers: Arc<GroupProviders>,
) {
    let info = replica.replica_info();
    let group_id = info.group_id;
    let replica_id = info.replica_id;
    drop(info);

    while let Ok(Some(current_term)) = replica.on_leader("scheduler", false).await {
        let providers: Vec<Arc<dyn EventSource>> = vec![
            group_providers.descriptor.clone(),
            group_providers.replica_states.clone(),
            group_providers.raft_state.clone(),
        ];
        let mut scheduler =
            Scheduler::new(cfg.clone(), replica.clone(), provider.clone(), providers);
        let mut pending_tasks = vec![];
        allocate_group_tasks(&mut pending_tasks, group_providers.clone()).await;
        if group_id == ROOT_GROUP_ID {
            setup_root_tasks(&mut pending_tasks).await;
        }
        scheduler.advance(current_term, pending_tasks).await;
        while let Ok(Some(term)) = replica.on_leader("scheduler", true).await {
            if term != current_term {
                break;
            }
            scheduler.wait_new_events().await;
            scheduler.advance(current_term, vec![]).await;
        }
    }
    debug!("group {group_id} replica {replica_id} scheduler is stopped");
}

async fn allocate_group_tasks(
    pending_tasks: &mut Vec<Box<dyn Task>>,
    providers: Arc<GroupProviders>,
) {
    use super::tasks::*;

    pending_tasks.push(Box::new(WatchReplicaStates::new(providers.clone())));
    pending_tasks.push(Box::new(WatchRaftState::new(providers.clone())));
    pending_tasks.push(Box::new(WatchGroupDescriptor::new(providers.clone())));
    pending_tasks.push(Box::new(PromoteGroup::new(providers.clone())));
    pending_tasks.push(Box::new(DurableGroup::new(providers.clone())));
    pending_tasks.push(Box::new(RemoveOrphanReplica::new(providers)));
}

#[allow(clippy::ptr_arg)]
async fn setup_root_tasks(_pending_tasks: &mut Vec<Box<dyn Task>>) {
    // TODO(walter) add root related scheduler tasks.
}
