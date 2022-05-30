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

use super::raft::{ApplyEntry, StateMachine};
use crate::{
    node::group_engine::{GroupEngine, WriteBatch},
    Result,
};

#[allow(unused)]
pub struct GroupStateMachine
where
    Self: Send,
{
    group_engine: GroupEngine,
}

impl GroupStateMachine {
    pub fn new(group_engine: GroupEngine) -> Self {
        GroupStateMachine { group_engine }
    }
}

#[allow(unused)]
impl StateMachine for GroupStateMachine {
    // FIXME(walter) support async?
    fn apply(&mut self, index: u64, term: u64, entry: ApplyEntry) -> crate::Result<()> {
        match entry {
            ApplyEntry::Empty => {}
            ApplyEntry::ConfigChange {} => {}
            ApplyEntry::Proposal { eval_result } => {
                if let Some(wb) = eval_result.batch {
                    let mut wb = WriteBatch::new(&wb.data);
                    self.group_engine.set_applied_index(&mut wb, index);
                    self.group_engine.commit(wb)?;
                }
            }
        }
        Ok(())
    }

    fn apply_snapshot(&mut self) -> Result<()> {
        todo!()
    }

    fn snapshot(&mut self) -> Result<()> {
        todo!()
    }

    fn flushed_index(&self) -> u64 {
        todo!()
    }
}
