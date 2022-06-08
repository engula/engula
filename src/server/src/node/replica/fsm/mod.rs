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

use engula_api::server::v1::{
    ChangeReplica, ChangeReplicaType, ChangeReplicas, GroupDesc, ReplicaDesc, ReplicaRole,
};
use tracing::trace;

use super::raft::{ApplyEntry, StateMachine};
use crate::{
    node::group_engine::{GroupEngine, WriteBatch},
    serverpb::v1::*,
    Result,
};

#[derive(Debug)]
enum ChangeReplicaKind {
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
    fn apply_change_replicas(&mut self, index: u64, change_replicas: ChangeReplicas) -> Result<()> {
        let mut wb = WriteBatch::default();
        let mut desc = self
            .group_engine
            .descriptor()
            .expect("GroupEngine::descriptor");
        match ChangeReplicaKind::new(&change_replicas) {
            ChangeReplicaKind::LeaveJoint => apply_leave_joint(&mut desc),
            ChangeReplicaKind::EnterJoint => apply_enter_joint(&mut desc, &change_replicas.changes),
            ChangeReplicaKind::Simple => {
                apply_simple_change(&mut desc, &change_replicas.changes[0])
            }
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
            ApplyEntry::ConfigChange { change_replicas } => {
                self.apply_change_replicas(index, change_replicas)?;
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

    fn descriptor(&self) -> GroupDesc {
        self.group_engine
            .descriptor()
            .expect("GroupEngine::descriptor")
    }
}

impl ChangeReplicaKind {
    fn new(cc: &ChangeReplicas) -> Self {
        match cc.changes.len() {
            0 => ChangeReplicaKind::LeaveJoint,
            1 => ChangeReplicaKind::Simple,
            _ => ChangeReplicaKind::EnterJoint,
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

fn apply_simple_change(desc: &mut GroupDesc, change: &ChangeReplica) {
    let replica_id = change.replica_id;
    let node_id = change.node_id;
    let exist = find_replica_mut(desc, replica_id);
    check_not_in_joint_state(&exist);
    match ChangeReplicaType::from_i32(change.change_type) {
        Some(ChangeReplicaType::Add) => {
            if let Some(replica) = exist {
                replica.role = ReplicaRole::Voter.into();
            } else {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id,
                    role: ReplicaRole::Voter.into(),
                });
            }
        }
        Some(ChangeReplicaType::AddLearner) => {
            if let Some(replica) = exist {
                replica.role = ReplicaRole::Learner.into();
            } else {
                desc.replicas.push(ReplicaDesc {
                    id: replica_id,
                    node_id,
                    role: ReplicaRole::Voter.into(),
                });
            }
        }
        Some(ChangeReplicaType::Remove) => {
            desc.replicas.drain_filter(|rep| rep.id == replica_id);
        }
        None => {
            panic!("such change replica operation isn't supported")
        }
    }
}

fn apply_enter_joint(desc: &mut GroupDesc, changes: &[ChangeReplica]) {
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
