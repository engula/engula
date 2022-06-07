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

use engula_api::server::v1::{GroupDesc, ReplicaDesc, ReplicaRole};
use raft::prelude::{ConfChangeSingle, ConfChangeType, ConfChangeV2, ConfState};
use tracing::trace;

use super::raft::{ApplyEntry, StateMachine};
use crate::{
    node::group_engine::{GroupEngine, WriteBatch},
    serverpb::v1::*,
    Result,
};

#[derive(Debug)]
enum ConfChangeKind {
    Simple,
    EnterJoint,
    LeaveJoint,
}

pub struct GroupStateMachine
where
    Self: Send,
{
    flushed_index: u64,
    group_engine: GroupEngine,
}

impl GroupStateMachine {
    pub fn new(group_engine: GroupEngine) -> Self {
        let flushed_index = group_engine.flushed_index();
        GroupStateMachine {
            flushed_index,
            group_engine,
        }
    }
}

impl GroupStateMachine {
    fn apply_conf_change(&mut self, index: u64, cc: ConfChangeV2) -> Result<()> {
        let mut wb = WriteBatch::default();
        let mut desc = self
            .group_engine
            .descriptor()
            .expect("GroupEngine::descriptor");
        match ConfChangeKind::new(&cc) {
            ConfChangeKind::LeaveJoint => apply_leave_joint(&mut desc),
            ConfChangeKind::EnterJoint => apply_enter_joint(&mut desc, &cc.changes),
            ConfChangeKind::Simple => apply_simple_change(&mut desc, &cc.changes[0]),
        }
        self.group_engine.set_applied_index(&mut wb, index);
        self.group_engine.set_group_desc(&mut wb, &desc);
        self.group_engine.commit(wb, false)?;

        Ok(())
    }

    fn apply_proposal(&mut self, index: u64, eval_result: EvalResult) -> Result<()> {
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

impl StateMachine for GroupStateMachine {
    // FIXME(walter) support async?
    fn apply(&mut self, index: u64, _term: u64, entry: ApplyEntry) -> Result<()> {
        trace!("apply entry index {} term {}", index, _term);
        match entry {
            ApplyEntry::Empty => {}
            ApplyEntry::ConfigChange { conf_change: cc } => {
                self.apply_conf_change(index, cc)?;
            }
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
        // TODO(walter) update flushed index in periodic.
        self.flushed_index
    }

    fn conf_state(&self) -> ConfState {
        let desc = self
            .group_engine
            .descriptor()
            .expect("GroupEngine::descriptor");
        let mut cs = ConfState::default();
        let mut in_joint = false;
        for replica in desc.replicas.iter() {
            match ReplicaRole::from_i32(replica.role).unwrap_or(ReplicaRole::Voter) {
                ReplicaRole::Voter => {
                    cs.voters.push(replica.id);
                    cs.voters_outgoing.push(replica.id);
                }
                ReplicaRole::Learner => {
                    cs.learners.push(replica.id);
                }
                ReplicaRole::IncomingVoter => {
                    in_joint = true;
                    cs.voters.push(replica.id);
                }
                ReplicaRole::DemotingVoter => {
                    in_joint = true;
                    cs.voters_outgoing.push(replica.id);
                    cs.learners_next.push(replica.id);
                }
            }
        }
        if !in_joint {
            cs.voters_outgoing.clear();
        }
        cs
    }
}

impl ConfChangeKind {
    fn new(cc: &ConfChangeV2) -> Self {
        match cc.changes.len() {
            0 => ConfChangeKind::LeaveJoint,
            1 => ConfChangeKind::Simple,
            _ => ConfChangeKind::EnterJoint,
        }
    }
}

fn apply_leave_joint(desc: &mut GroupDesc) {
    for replica in &mut desc.replicas {
        let new_role = match ReplicaRole::from_i32(replica.role) {
            Some(ReplicaRole::IncomingVoter) => ReplicaRole::Voter.into(),
            Some(ReplicaRole::DemotingVoter) => ReplicaRole::Learner.into(),
            _ => replica.role,
        };
        replica.role = new_role;
    }
}

fn apply_simple_change(desc: &mut GroupDesc, change: &ConfChangeSingle) {
    let replica_id = change.node_id;
    let exist = find_replica_mut(desc, replica_id);
    check_not_in_joint_state(&exist);
    match change.get_change_type() {
        ConfChangeType::AddNode => {
            if let Some(replica) = exist {
                replica.role = ReplicaRole::Voter.into();
            } else {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id: 0, // FIXME(walter)
                    role: ReplicaRole::Voter.into(),
                });
            }
        }
        ConfChangeType::AddLearnerNode => {
            if let Some(replica) = exist {
                replica.role = ReplicaRole::Learner.into();
            } else {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id: 0, // FIXME(walter)
                    role: ReplicaRole::Voter.into(),
                });
            }
        }
        ConfChangeType::RemoveNode => {
            desc.replicas.drain_filter(|rep| rep.id == replica_id);
        }
    }
}

fn apply_enter_joint(desc: &mut GroupDesc, changes: &[ConfChangeSingle]) {
    for change in changes {
        apply_simple_change(desc, change);
    }
}

fn find_replica_mut(desc: &mut GroupDesc, replica_id: u64) -> Option<&mut ReplicaDesc> {
    desc.replicas.iter_mut().find(|rep| rep.id == replica_id)
}

fn check_not_in_joint_state(exist: &Option<&mut ReplicaDesc>) {
    if matches!(
        exist
            .as_ref()
            .and_then(|rep| ReplicaRole::from_i32(rep.role))
            .unwrap_or(ReplicaRole::Voter),
        ReplicaRole::IncomingVoter | ReplicaRole::DemotingVoter
    ) {
        panic!("execute conf change but still in joint state");
    }
}
