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

use engula_api::server::v1::ScheduleState;
use tracing::debug;

use super::ScheduleStateObserver;
use crate::{
    node::{replica::ReplicaConfig, Replica},
    runtime::{sync::WaitGroup, TaskPriority},
    schedule::{
        event_source::EventSource,
        provider::{GroupProviders, MoveReplicasProvider},
        scheduler::Scheduler,
        task::Task,
    },
    Provider,
};

pub(crate) fn setup_scheduler(
    cfg: ReplicaConfig,
    provider: Arc<Provider>,
    replica: Arc<Replica>,
    move_replicas_provider: Arc<MoveReplicasProvider>,
    schedule_state_observer: Arc<dyn ScheduleStateObserver>,
    wait_group: WaitGroup,
) {
    let group_providers = Arc::new(GroupProviders::new(
        replica.clone(),
        provider.router.clone(),
        move_replicas_provider,
    ));

    let group_id = replica.replica_info().group_id;
    let tag = &group_id.to_le_bytes();
    let executor = crate::runtime::current();
    executor.spawn(Some(tag), TaskPriority::Low, async move {
        scheduler_main(
            cfg,
            replica,
            provider,
            group_providers,
            schedule_state_observer,
        )
        .await;
        drop(wait_group);
    });
}

async fn scheduler_main(
    cfg: ReplicaConfig,
    replica: Arc<Replica>,
    provider: Arc<Provider>,
    group_providers: Arc<GroupProviders>,
    schedule_state_observer: Arc<dyn ScheduleStateObserver>,
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
            group_providers.move_replicas.clone(),
        ];
        let mut scheduler = Scheduler::new(
            cfg.clone(),
            replica.clone(),
            provider.clone(),
            providers,
            schedule_state_observer.clone(),
        );
        allocate_group_tasks(&mut scheduler, group_providers.clone()).await;

        // After the schedule is initialized, the root needs to be notified to clear the expired
        // state in memory.
        schedule_state_observer.on_schedule_state_updated(ScheduleState::default());
        while let Ok(Some(term)) = replica.on_leader("scheduler", true).await {
            if term != current_term {
                break;
            }
            scheduler.advance(current_term).await;
        }
    }
    debug!("group {group_id} replica {replica_id} scheduler is stopped");
}

async fn allocate_group_tasks(scheduler: &mut Scheduler, providers: Arc<GroupProviders>) {
    use super::tasks::*;

    let tasks: Vec<Box<dyn Task>> = vec![
        Box::new(WatchReplicaStates::new(providers.clone())),
        Box::new(WatchRaftState::new(providers.clone())),
        Box::new(WatchGroupDescriptor::new(providers.clone())),
        Box::new(PromoteGroup::new(providers.clone())),
        Box::new(DurableGroup::new(providers.clone())),
        Box::new(RemoveOrphanReplica::new(providers.clone())),
        Box::new(ReplicaMigration::new(providers)),
    ];
    scheduler.install_tasks(tasks);
}
