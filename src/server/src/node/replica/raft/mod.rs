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
mod applier;
mod facade;
mod fsm;
mod node;
mod snap;
mod storage;
mod transport;
mod worker;

use std::{path::Path, sync::Arc};

use engula_api::server::v1::{
    ChangeReplicaType, ChangeReplicas, GroupDesc, ReplicaDesc, ReplicaRole,
};
use raft::prelude::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};

use self::snap::SnapManager;
pub use self::{
    facade::RaftNodeFacade,
    fsm::{ApplyEntry, StateMachine, Checkpoint},
    storage::write_initial_state,
    transport::{AddressResolver, TransportManager},
    worker::StateObserver,
};
use crate::{
    node::replica::raft::worker::RaftWorker,
    runtime::{Executor, TaskPriority},
    Result,
};

/// `ReadPolicy` is used to control `RaftNodeFacade::read` behavior.
#[derive(Debug)]
#[allow(unused)]
pub enum ReadPolicy {
    /// Do nothing
    Relaxed,
    /// Wait until all former committed entries be applied.
    LeaseRead,
    /// Like `ReadPolicy::LeaseRead`, but require exchange heartbeat with majority members before
    /// waiting.
    ReadIndex,
}

#[derive(Clone)]
pub struct RaftManager {
    executor: Executor,
    engine: Arc<raft_engine::Engine>,
    transport_mgr: TransportManager,
    snap_mgr: SnapManager,
}

impl RaftManager {
    pub fn open<P: AsRef<Path>>(
        path: P,
        executor: Executor,
        transport_mgr: TransportManager,
    ) -> Result<Self> {
        use raft_engine::{Config, Engine};
        let engine_dir = path.as_ref().join("engine");
        let snap_dir = path.as_ref().join("snap");
        let cfg = Config {
            dir: engine_dir.to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(cfg)?);
        let snap_mgr = SnapManager::recovery(snap_dir)?;
        Ok(RaftManager {
            executor,
            engine,
            transport_mgr,
            snap_mgr,
        })
    }

    #[inline]
    pub fn engine(&self) -> &raft_engine::Engine {
        &self.engine
    }

    #[inline]
    pub async fn list_groups(&self) -> Vec<u64> {
        self.engine.raft_groups()
    }

    pub async fn start_raft_group<M: 'static + StateMachine>(
        &self,
        group_id: u64,
        desc: ReplicaDesc,
        state_machine: M,
        observer: Box<dyn StateObserver>,
    ) -> Result<RaftNodeFacade> {
        // TODO(walter) config channel size.
        let worker = RaftWorker::open(group_id, desc, state_machine, self, observer).await?;
        let facade = RaftNodeFacade::open(worker.request_sender());
        self.executor.spawn(None, TaskPriority::High, async move {
            // TODO(walter) handle result.
            worker.run().await.unwrap();
        });
        Ok(facade)
    }
}

fn encode_to_conf_change(change_replicas: ChangeReplicas) -> ConfChangeV2 {
    use prost::Message;

    let mut conf_changes = vec![];
    for c in &change_replicas.changes {
        let change_type = match ChangeReplicaType::from_i32(c.change_type) {
            Some(ChangeReplicaType::Add) => ConfChangeType::AddNode,
            Some(ChangeReplicaType::Remove) => ConfChangeType::RemoveNode,
            Some(ChangeReplicaType::AddLearner) => ConfChangeType::AddLearnerNode,
            None => panic!("such change replica operation isn't supported"),
        };
        conf_changes.push(ConfChangeSingle {
            change_type: change_type.into(),
            node_id: c.replica_id,
        });
    }

    ConfChangeV2 {
        transition: ConfChangeTransition::Auto.into(),
        context: change_replicas.encode_to_vec(),
        changes: conf_changes,
    }
}

fn decode_from_conf_change(conf_change: &ConfChangeV2) -> ChangeReplicas {
    use prost::Message;

    ChangeReplicas::decode(&*conf_change.context)
        .expect("ChangeReplicas is saved in ConfChangeV2::context")
}

pub fn conf_state_from_group_descriptor(desc: &GroupDesc) -> ConfState {
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
