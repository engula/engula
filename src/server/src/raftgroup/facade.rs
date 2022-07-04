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

use super::{worker::Request, ReadPolicy};
use crate::{
    serverpb::v1::{EvalResult, RaftMessage},
    Result,
};

/// `RaftNodeFacade` wraps the operations of raft.
#[derive(Clone)]
pub struct RaftNodeFacade
where
    Self: Send,
{
    request_sender: mpsc::Sender<Request>,
}

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
    pub async fn propose(&mut self, eval_result: EvalResult) -> Result<()> {
        let (sender, receiver) = oneshot::channel();

        let request = Request::Propose {
            eval_result,
            sender,
        };

        self.send(request)?;
        receiver.await?
    }

    /// Execute reading operations with the specified read policy.
    pub async fn read(&mut self, policy: ReadPolicy) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        if matches!(policy, ReadPolicy::Relaxed) {
            sender.send(Ok(())).unwrap_or_default();
        } else {
            self.send(Request::Read { policy, sender })?;
        }

        receiver.await?
    }

    /// Step raft messages.
    pub fn step(&mut self, msg: RaftMessage) -> Result<()> {
        self.send(Request::Message(msg))
    }

    pub fn transfer_leader(&mut self, transferee: u64) -> Result<()> {
        self.send(Request::Transfer { transferee })
    }

    pub async fn change_config(&mut self, change: ChangeReplicas) -> Result<()> {
        let (sender, receiver) = oneshot::channel();

        let request = Request::ChangeConfig { change, sender };
        self.send(request)?;

        receiver.await?
    }

    pub fn report_unreachable(&mut self, target_id: u64) {
        self.send(Request::Unreachable { target_id })
            .unwrap_or_default()
    }

    fn send(&mut self, req: Request) -> Result<()> {
        use crate::Error;

        if self.request_sender.try_send(req).is_err() {
            // The target raft group is shutdown.
            return Err(Error::ServiceIsBusy("raft"));
        }

        Ok(())
    }
}
