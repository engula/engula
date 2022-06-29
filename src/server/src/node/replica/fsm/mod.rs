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

mod checkpoint;

use std::{path::Path, sync::Arc};

use engula_api::server::v1::{
    ChangeReplica, ChangeReplicaType, ChangeReplicas, GroupDesc, MigrationDesc, ReplicaDesc,
    ReplicaRole,
};
use tracing::{info, trace, warn};

use super::ReplicaInfo;
use crate::{
    node::engine::{GroupEngine, WriteBatch},
    raftgroup::{ApplyEntry, SnapshotBuilder, StateMachine},
    serverpb::v1::*,
    Result,
};

#[derive(Debug)]
enum ChangeReplicaKind {
    Simple,
    EnterJoint,
    LeaveJoint,
}

/// An abstracted structure used to subscribe to state machine changes.
pub trait StateMachineObserver: Send + Sync {
    /// This function will be called every time the `GroupDesc` changes.
    fn on_descriptor_updated(&mut self, descriptor: GroupDesc);

    /// This function will be called once the encountered term changes.
    fn on_term_updated(&mut self, term: u64);

    /// This function will be called once the migrate state changes.
    fn on_migrate_state_updated(&mut self, migrate_state: Option<MigrationState>);
}

pub struct GroupStateMachine
where
    Self: Send,
{
    info: Arc<ReplicaInfo>,

    group_engine: GroupEngine,
    observer: Box<dyn StateMachineObserver>,

    /// Whether `GroupDesc` changes during apply.
    desc_updated: bool,
    migration_state_updated: bool,
    last_applied_term: u64,
}

impl GroupStateMachine {
    pub fn new(
        info: Arc<ReplicaInfo>,
        group_engine: GroupEngine,
        observer: Box<dyn StateMachineObserver>,
    ) -> Self {
        let apply_state = group_engine
            .flushed_apply_state()
            .expect("access flushed index");
        GroupStateMachine {
            info,
            group_engine,
            observer,
            desc_updated: false,
            migration_state_updated: false,
            last_applied_term: apply_state.term,
        }
    }
}

impl GroupStateMachine {
    fn apply_change_replicas(
        &mut self,
        index: u64,
        term: u64,
        change_replicas: ChangeReplicas,
    ) -> Result<()> {
        let mut wb = WriteBatch::default();
        let mut desc = self.group_engine.descriptor();
        match ChangeReplicaKind::new(&change_replicas) {
            ChangeReplicaKind::LeaveJoint => apply_leave_joint(&mut desc),
            ChangeReplicaKind::EnterJoint => apply_enter_joint(&mut desc, &change_replicas.changes),
            ChangeReplicaKind::Simple => {
                apply_simple_change(&mut desc, &change_replicas.changes[0])
            }
        }
        desc.epoch += 1;
        self.desc_updated = true;
        self.group_engine.set_apply_state(&mut wb, index, term);
        self.group_engine.set_group_desc(&mut wb, &desc);
        self.group_engine.commit(wb, false)?;

        Ok(())
    }

    fn apply_proposal(&mut self, index: u64, term: u64, eval_result: EvalResult) -> Result<()> {
        let mut wb = if let Some(wb) = eval_result.batch {
            WriteBatch::new(&wb.data)
        } else {
            WriteBatch::default()
        };

        if let Some(op) = eval_result.op {
            let mut desc = self.group_engine.descriptor();
            if let Some(AddShard { shard: Some(shard) }) = op.add_shard {
                for existed_shard in &desc.shards {
                    if existed_shard.id == shard.id {
                        todo!("shard {} already existed in group", shard.id);
                    }
                }
                self.desc_updated = true;
                desc.epoch += 1;
                desc.shards.push(shard);
            }
            if let Some(m) = op.migration {
                self.apply_migration_event(&mut wb, m, &mut desc);
            }

            // Any sync_op will update group desc.
            self.group_engine.set_group_desc(&mut wb, &desc);
        }

        self.group_engine.set_apply_state(&mut wb, index, term);
        self.group_engine.commit(wb, false)?;

        Ok(())
    }

    fn apply_migration_event(
        &mut self,
        wb: &mut WriteBatch,
        migration: Migration,
        group_desc: &mut GroupDesc,
    ) {
        let event = MigrationEvent::from_i32(migration.event).expect("unknown migration event");
        if let Some(desc) = migration.migration_desc.as_ref() {
            info!(
                replica = self.info.replica_id,
                group = self.info.group_id,
                %desc,
                ?event,
                "apply migration event"
            );
        }

        match event {
            MigrationEvent::Setup => {
                if migration.migration_desc.is_none() {
                    warn!(
                        replica_id = self.info.replica_id,
                        group = self.info.group_id,
                        "Migration::migration_desc is None"
                    );
                    return;
                }

                let state = MigrationState {
                    migration_desc: migration.migration_desc,
                    last_migrated_key: vec![],
                    step: MigrationStep::Prepare as i32,
                };
                debug_assert!(state.migration_desc.is_some());
                self.group_engine.set_migration_state(wb, &state);
                self.migration_state_updated = true;
            }
            MigrationEvent::Ingest => {
                let mut state = self.must_migration_state();

                // If only the ingested key changes, there is no need to notify the migration
                // controller to perform corresponding operations.
                if state.step == MigrationStep::Prepare as i32 {
                    state.step = MigrationStep::Migrating as i32;
                    self.migration_state_updated = true;
                }

                debug_assert!(state.step == MigrationStep::Migrating as i32);
                state.last_migrated_key = migration.last_ingested_key;

                self.group_engine.set_migration_state(wb, &state);
            }
            MigrationEvent::Commit => {
                let mut state = self.must_migration_state();
                debug_assert!(
                    state.step == MigrationStep::Migrating as i32
                        || state.step == MigrationStep::Prepare as i32
                );
                state.step = MigrationStep::Migrated as i32;
                self.group_engine.set_migration_state(wb, &state);
                self.migration_state_updated = true;
            }
            MigrationEvent::Apply => {
                let mut state = self.must_migration_state();
                debug_assert!(state.step == MigrationStep::Migrated as i32);

                let desc = state.get_migration_desc();
                self.apply_migration(group_desc, desc);

                state.step = MigrationStep::Finished as i32;
                self.group_engine.set_migration_state(wb, &state);
                self.migration_state_updated = true;
            }
            MigrationEvent::Abort => {
                let mut state = self.must_migration_state();
                debug_assert!(state.step == MigrationStep::Prepare as i32);

                state.step = MigrationStep::Aborted as i32;
                self.group_engine.set_migration_state(wb, &state);
                self.migration_state_updated = true;
            }
        }
    }

    fn apply_migration(&mut self, group_desc: &mut GroupDesc, desc: &MigrationDesc) {
        let shard_desc = desc.get_shard_desc();

        group_desc.epoch += 1;
        let msg = if desc.src_group_id == group_desc.id {
            group_desc.shards.drain_filter(|r| r.id == shard_desc.id);
            "shard migrated out"
        } else {
            debug_assert_eq!(desc.dest_group_id, group_desc.id);
            group_desc.shards.push(shard_desc.clone());
            "shard migrated in"
        };
        info!(
            replica = self.info.replica_id,
            group = self.info.group_id,
            epoch = group_desc.epoch,
            shard = shard_desc.id,
            msg
        );
        self.desc_updated = true;
    }

    fn flush_updated_events(&mut self, term: u64) {
        if self.desc_updated {
            self.desc_updated = false;
            self.observer
                .on_descriptor_updated(self.group_engine.descriptor());
        }

        if term > self.last_applied_term {
            self.last_applied_term = term;
            self.observer.on_term_updated(term);
        }

        if self.migration_state_updated {
            self.migration_state_updated = false;
            self.observer
                .on_migrate_state_updated(self.group_engine.migration_state());
        }
    }

    #[inline]
    fn flushed_apply_state(&self) -> ApplyState {
        self.group_engine
            .flushed_apply_state()
            .expect("access flushed index")
    }

    #[inline]
    fn must_migration_state(&self) -> MigrationState {
        self.group_engine
            .migration_state()
            .expect("The MigrationState should exist")
    }
}

impl StateMachine for GroupStateMachine {
    // FIXME(walter) support async?
    fn apply(&mut self, index: u64, term: u64, entry: ApplyEntry) -> Result<()> {
        trace!("apply entry index {} term {}", index, term);
        match entry {
            ApplyEntry::Empty => {}
            ApplyEntry::ConfigChange { change_replicas } => {
                self.apply_change_replicas(index, term, change_replicas)?;
            }
            ApplyEntry::Proposal { eval_result } => {
                self.apply_proposal(index, term, eval_result)?;
            }
        }

        self.flush_updated_events(term);

        Ok(())
    }

    fn apply_snapshot(&mut self, snap_dir: &Path) -> Result<()> {
        checkpoint::apply_snapshot(&self.group_engine, snap_dir)?;
        self.observer
            .on_descriptor_updated(self.group_engine.descriptor());
        let apply_state = self.flushed_apply_state();
        self.observer.on_term_updated(apply_state.term);
        Ok(())
    }

    fn snapshot_builder(&self) -> Box<dyn SnapshotBuilder> {
        Box::new(checkpoint::GroupSnapshotBuilder::new(
            self.group_engine.clone(),
        ))
    }

    #[inline]
    fn flushed_index(&self) -> u64 {
        // FIXME(walter) avoid disk IO.
        self.group_engine
            .flushed_apply_state()
            .expect("access flushed index")
            .index
    }

    #[inline]
    fn descriptor(&self) -> GroupDesc {
        self.group_engine.descriptor()
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
