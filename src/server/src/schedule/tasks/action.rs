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

use crate::schedule::{
    actions::{Action, ActionState},
    scheduler::ScheduleContext,
    task::{Task, TaskState},
};

#[derive(Clone, Copy, Debug)]
enum ActionStage {
    Next(usize),
    Doing(usize),
}

pub struct ActionTask {
    task_id: u64,
    stage: ActionStage,
    actions: Vec<Box<dyn Action>>,
}

impl ActionTask {
    pub fn new(task_id: u64, actions: Vec<Box<dyn Action>>) -> Self {
        ActionTask {
            task_id,
            stage: ActionStage::Next(0),
            actions,
        }
    }
}

#[crate::async_trait]
impl Task for ActionTask {
    fn id(&self) -> u64 {
        self.task_id
    }

    async fn poll(&mut self, ctx: &mut ScheduleContext<'_>) -> TaskState {
        match self.stage {
            ActionStage::Next(index) if index >= self.actions.len() => TaskState::Terminated,
            ActionStage::Next(index) => match self.actions[index].setup(self.task_id, ctx).await {
                ActionState::Pending(interval) => TaskState::Pending(interval),
                ActionState::Aborted => TaskState::Terminated,
                ActionState::Done => {
                    self.stage = ActionStage::Doing(index);
                    TaskState::Advanced
                }
            },
            ActionStage::Doing(index) => match self.actions[index].poll(self.task_id, ctx).await {
                ActionState::Pending(interval) => TaskState::Pending(interval),
                ActionState::Aborted => TaskState::Terminated,
                ActionState::Done => {
                    self.stage = ActionStage::Next(index + 1);
                    TaskState::Advanced
                }
            },
        }
    }
}
