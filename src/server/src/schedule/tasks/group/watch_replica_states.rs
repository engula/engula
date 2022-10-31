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
use std::{sync::Arc, time::Duration};

use tracing::{error, trace};

use crate::{
    root::RemoteStore,
    schedule::{
        provider::GroupProviders,
        scheduler::ScheduleContext,
        task::{Task, TaskState},
        tasks::WATCH_REPLICA_STATES_TASK_ID,
    },
};

pub struct WatchReplicaStates {
    providers: Arc<GroupProviders>,
}

impl WatchReplicaStates {
    pub fn new(providers: Arc<GroupProviders>) -> Self {
        WatchReplicaStates { providers }
    }
}

#[crate::async_trait]
impl Task for WatchReplicaStates {
    fn id(&self) -> u64 {
        WATCH_REPLICA_STATES_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        let group_id = ctx.group_id;
        let root_store = RemoteStore::new(ctx.transport_manager.clone());
        match root_store.list_replica_state(group_id).await {
            Ok(states) => {
                trace!("group {group_id} list replica states: {:?}", states);
                self.providers.replica_states.update(states);
            }
            Err(e) => {
                error!("group {group_id} watch replica states: {e:?}");
            }
        }

        if ctx
            .cfg
            .testing_knobs
            .disable_scheduler_orphan_replica_detecting_intervals
        {
            TaskState::Pending(Some(Duration::from_millis(1)))
        } else {
            TaskState::Pending(Some(Duration::from_secs(31)))
        }
    }
}
