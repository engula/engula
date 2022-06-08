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

pub use self::{
    facade::RaftNodeFacade,
    fsm::{ApplyEntry, StateMachine},
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
}

impl RaftManager {
    pub fn open<P: AsRef<Path>>(
        path: P,
        executor: Executor,
        transport_mgr: TransportManager,
    ) -> Result<Self> {
        use raft_engine::{Config, Engine};
        let cfg = Config {
            dir: path.as_ref().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(cfg)?);
        Ok(RaftManager {
            executor,
            engine,
            transport_mgr,
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
        replica_id: u64,
        state_machine: M,
        observer: Box<dyn StateObserver>,
    ) -> Result<RaftNodeFacade> {
        let worker = RaftWorker::open(group_id, replica_id, state_machine, self, observer).await?;
        let facade = RaftNodeFacade::open(worker.request_sender());
        self.executor.spawn(None, TaskPriority::High, async move {
            // TODO(walter) handle result.
            worker.run().await.unwrap();
        });
        Ok(facade)
    }
}
