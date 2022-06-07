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

use engula_api::server::v1::ChangeReplicas;
use futures::channel::{mpsc, oneshot};
use raft::prelude::*;

use super::{worker::Request, ReadPolicy};
use crate::{serverpb::v1::EvalResult, Result};

/// `RaftNodeFacade` wraps the operations of raft.
#[derive(Clone)]
pub struct RaftNodeFacade
where
    Self: Send,
{
    request_sender: mpsc::Sender<Request>,
}

#[allow(unused)]
impl RaftNodeFacade {
    /// Open the existed raft node.
    pub fn open(sender: mpsc::Sender<Request>) -> Self {
        RaftNodeFacade {
            request_sender: sender,
        }
    }

    /// Submit a data to replicate, and returns corresponding future value.
    ///
    /// Once the data is applied to the [`StateMachine`], the value of future will be set to
    /// [`Ok(())`]. The future is set to specific error if the data cannot be applied.
    ///
    /// TODO(walter) support return user defined error.
    pub fn propose(&mut self, eval_result: EvalResult) -> oneshot::Receiver<Result<()>> {
        let (sender, receiver) = oneshot::channel();

        let request = Request::Propose {
            eval_result,
            sender,
        };
        match self.request_sender.try_send(request) {
            Ok(()) => (),
            Err(err) => {
                todo!("handle error: {:?}", err)
            }
        }

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
        let (sender, receiver) = oneshot::channel();
        if matches!(policy, ReadPolicy::Relaxed) {
            sender.send(Ok(())).unwrap_or_default();
        } else {
            let request = Request::Read { policy, sender };
            match self.request_sender.try_send(request) {
                Ok(()) => (),
                Err(e) => {
                    todo!("handle error: {:?}", e);
                }
            }
        }

        receiver
    }

    /// Step raft messages.
    pub fn step(&mut self, msg: Message) -> Result<()> {
        match self.request_sender.try_send(Request::Message(msg)) {
            Ok(()) => (),
            Err(e) => {
                todo!("handle error: {:?}", e);
            }
        }
        Ok(())
    }

    /// Acquire the latest snapshot, create it no such snapshot exists.
    pub fn acquire_snapshot(&mut self) -> oneshot::Receiver<Result<u64>> {
        todo!()
    }

    pub fn transfer_leader(&mut self, target_id: u64) -> Result<()> {
        let request = Request::Transfer { target_id };
        match self.request_sender.try_send(request) {
            Ok(()) => Ok(()),
            Err(err) => {
                todo!("handle error: {:?}", err)
            }
        }
    }

    pub fn change_config(&mut self, change: ChangeReplicas) -> oneshot::Receiver<Result<()>> {
        let (sender, receiver) = oneshot::channel();

        let request = Request::ChangeConfig { change, sender };
        match self.request_sender.try_send(request) {
            Ok(()) => (),
            Err(err) => {
                todo!("handle error: {:?}", err)
            }
        }

        receiver
    }

    pub fn report_unreachable(&mut self, target_id: u64) {
        self.request_sender
            .try_send(Request::Unreachable { target_id })
            .unwrap_or_default();
    }
}
