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
mod act_config_change;
mod act_replica;

use std::time::Duration;

pub(crate) use self::{
    act_config_change::{AddLearners, RemoveLearners, ReplaceVoters},
    act_replica::{ClearReplicaState, CreateReplicas, RemoveReplica},
};
use super::scheduler::ScheduleContext;

#[derive(Debug)]
pub enum ActionState {
    Pending(Option<Duration>),
    Aborted,
    Done,
}

/// An abstraction which used to describe a independent behavior and it's terminated states.
///
/// The working model of `Action`s are issuing requests and polling metadata changes. For example,
/// adding learners and promoting them to voters requires the addition learners catch up leader's
/// committed entries, so the `setup` should add the learners into raft group and `poll` should wait
/// until the learners catch up log entries.
#[crate::async_trait]
pub trait Action: Send {
    #[allow(unused)]
    async fn setup(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        ActionState::Done
    }

    #[allow(unused)]
    async fn poll(&mut self, task_id: u64, ctx: &mut ScheduleContext<'_>) -> ActionState {
        ActionState::Done
    }
}
