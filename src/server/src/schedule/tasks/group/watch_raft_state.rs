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

use crate::schedule::{
    provider::GroupProviders,
    scheduler::ScheduleContext,
    task::{Task, TaskState},
    tasks::WATCH_RAFT_STATE_TASK_ID,
};

pub struct WatchRaftState {
    providers: Arc<GroupProviders>,
}

impl WatchRaftState {
    pub fn new(providers: Arc<GroupProviders>) -> Self {
        WatchRaftState { providers }
    }
}

#[crate::async_trait]
impl Task for WatchRaftState {
    fn id(&self) -> u64 {
        WATCH_RAFT_STATE_TASK_ID
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        if let Some(states) = ctx.replica.raft_node().raft_group_state().await {
            self.providers.raft_state.update(states);
        }
        TaskState::Pending(Some(Duration::from_secs(1)))
    }
}
