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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use engula_api::server::v1::*;
use tracing::info;

use crate::schedule::{
    actions::{Action, ClearReplicaState, RemoveReplica},
    provider::GroupProviders,
    scheduler::ScheduleContext,
    task::{Task, TaskState},
    tasks::{ActionTask, REMOVE_ORPHAN_REPLICA_TASK_ID},
};

pub struct RemoveOrphanReplica {
    orphan_replicas: HashMap<u64, Instant>,
    providers: Arc<GroupProviders>,
}

impl RemoveOrphanReplica {
    pub(crate) fn new(providers: Arc<GroupProviders>) -> RemoveOrphanReplica {
        RemoveOrphanReplica {
            orphan_replicas: HashMap::default(),
            providers,
        }
    }

    async fn setup_removing_task(
        &mut self,
        ctx: &mut ScheduleContext<'_>,
        replica_id: u64,
        node_id: u64,
        desc: GroupDesc,
    ) {
        let replica = ReplicaDesc {
            id: replica_id,
            node_id,
            ..Default::default()
        };

        let remove_replica: Box<dyn Action> = Box::new(RemoveReplica {
            group: desc,
            replica: replica,
        });
        let clear_replica_state: Box<dyn Action> = Box::new(ClearReplicaState {
            target_id: replica_id,
        });

        let task = Box::new(ActionTask::new(
            ctx.next_task_id(),
            vec![remove_replica, clear_replica_state],
        ));
        ctx.delegate(task);
        self.orphan_replicas.remove(&replica_id);
    }
}

#[crate::async_trait]
impl Task for RemoveOrphanReplica {
    fn id(&self) -> u64 {
        REMOVE_ORPHAN_REPLICA_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        let now = Instant::now();
        let replica_states = self.providers.replica_states.replica_states();
        let desc = self.providers.descriptor.descriptor();
        for s in &replica_states {
            let instant = match self.orphan_replicas.get(&s.replica_id) {
                Some(instant) => instant,
                None => continue,
            };
            if *instant + Duration::from_secs(60) > now
                && !ctx
                    .cfg
                    .testing_knobs
                    .disable_orphan_replica_detecting_intervals
            {
                continue;
            }

            let replica_id = s.replica_id;
            let group_id = s.group_id;
            for r in &desc.replicas {
                if r.id == replica_id {
                    panic!(
                        "replica {replica_id} belongs to group {group_id}, it must not a orphan replica",
                    );
                }
            }

            info!("group {group_id} find a orphan replica {replica_id}, try remove it");

            self.setup_removing_task(ctx, replica_id, s.node_id, desc.clone())
                .await;
        }

        TaskState::Pending(Some(Duration::from_secs(60)))
    }
}
