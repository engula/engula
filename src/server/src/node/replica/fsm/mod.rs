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
    serverpb::v1::*,
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

impl GroupStateMachine {
    fn apply_proposal(&mut self, index: u64, eval_result: EvalResult) -> crate::Result<()> {
        let mut wb = if let Some(wb) = eval_result.batch {
            WriteBatch::new(&wb.data)
        } else {
            WriteBatch::default()
        };

        if let Some(op) = eval_result.op {
            let mut desc = self
                .group_engine
                .descriptor()
                .expect("GroupEngine::descriptor");
            if let Some(AddShard { shard: Some(shard) }) = op.add_shard {
                for existed_shard in &desc.shards {
                    if existed_shard.id == shard.id {
                        todo!("shard {} already existed in group", shard.id);
                    }
                }
                desc.shards.push(shard);
            }
            self.group_engine.set_group_desc(&mut wb, &desc);
        }

        self.group_engine.set_applied_index(&mut wb, index);
        self.group_engine.commit(wb, false)?;

        Ok(())
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
                self.apply_proposal(index, eval_result)?;
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
