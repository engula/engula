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
mod fsm;
mod snap;
mod state;
mod transport;

use std::sync::{Arc, Mutex};

use futures::channel::oneshot;
use raft::{prelude::*, StateRole};

pub use self::{
    fsm::{ApplyEntry, StateMachine},
    state::StateObserver,
};
use crate::{serverpb::v1::EvalResult, Result};

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

/// `RaftNodeFacade` wraps the operations of raft.
#[derive(Clone)]
#[allow(unused)]
pub struct RaftNodeFacade
where
    Self: Send,
{
    fsm: Arc<Mutex<Box<dyn StateMachine + Send>>>,
    observer: Arc<Mutex<Box<dyn StateObserver + Send>>>,
}

#[allow(unused)]
impl RaftNodeFacade {
    /// Create new raft node.
    ///
    /// `replicas` specific the initial membership of raft group.
    pub async fn create(
        replica_id: u64,
        replicas: Vec<u64>,
        fsm: Box<dyn StateMachine + Send>,
    ) -> Result<()> {
        // TODO(walter) add implementation.
        Ok(())
    }

    /// Open the existed raft node.
    pub async fn open(
        replica_id: u64,
        fsm: Box<dyn StateMachine + Send>,
        mut observer: Box<dyn StateObserver + Send>,
    ) -> Result<Self> {
        // TODO(walter) add implementation.
        observer.on_state_updated(StateRole::Leader, 1);
        Ok(RaftNodeFacade {
            fsm: Arc::new(Mutex::new(fsm)),
            observer: Arc::new(Mutex::new(observer)),
        })
    }

    /// Submit a data to replicate, and returns corresponding future value.
    ///
    /// Once the data is applied to the [`StateMachine`], the value of future will be set to
    /// [`Ok(())`]. The future is set to specific error if the data cannot be applied.
    ///
    /// TODO(walter) support return user defined error.
    #[allow(unused)]
    pub fn propose(&self, eval_result: EvalResult) -> oneshot::Receiver<Result<()>> {
        self.fsm
            .lock()
            .unwrap()
            .apply(0, 0, ApplyEntry::Proposal { eval_result });
        let (sender, receiver) = oneshot::channel();
        sender.send(Ok(())).unwrap();
        receiver
    }

    /// Try to campaign leader.
    #[allow(unused)]
    pub fn campaign(&self) -> oneshot::Receiver<Result<()>> {
        todo!()
    }

    /// Execute reading operations with the specified read policy.
    #[allow(unused)]
    pub fn read(&mut self, policy: ReadPolicy) -> oneshot::Receiver<Result<()>> {
        todo!()
    }

    /// Step raft messages.
    #[allow(unused)]
    pub fn step(&mut self, msg: Message) -> Result<()> {
        todo!()
    }

    /// Acquire the latest snapshot, create it no such snapshot exists.
    pub fn acquire_snapshot(&mut self) -> oneshot::Receiver<Result<u64>> {
        todo!()
    }

    pub fn transfer_leader(&mut self, target_id: u64) -> Result<()> {
        todo!()
    }

    pub fn change_config(&mut self, change: ConfChangeV2) -> oneshot::Receiver<Result<()>> {
        todo!()
    }
}
