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

use futures::channel::oneshot;
use raft::{prelude::*, ConfChangeI, StateRole};

use super::{applier::Applier, fsm::StateMachine, storage::Storage, RaftManager};
use crate::{Error, Result};

/// WriteTask records the metadata and entries to persist to disk.
#[derive(Default)]
pub struct WriteTask {
    pub hard_state: Option<HardState>,
    pub entries: Vec<Entry>,

    /// Snapshot specifies the snapshot to be saved to stable storage.
    pub snapshot: Option<Snapshot>,

    post_ready: PostReady,
}

#[derive(Default)]
pub struct PostReady {
    /// The number of ready. See `raft::raw_node::Ready` for details.
    number: u64,

    persisted_messages: Vec<Message>,
}

pub trait AdvanceTemplate {
    fn send_messages(&mut self, msgs: Vec<Message>);

    fn on_state_updated(&mut self, leader_id: u64, term: u64, role: StateRole);
}

pub struct RaftNode<M: StateMachine> {
    group_id: u64,

    lease_read_requests: Vec<oneshot::Sender<Result<()>>>,
    read_index_requests: Vec<oneshot::Sender<Result<()>>>,

    raw_node: RawNode<Storage>,
    applier: Applier<M>,
}

impl<M> RaftNode<M>
where
    M: StateMachine,
{
    pub async fn new(
        group_id: u64,
        replica_id: u64,
        mgr: &RaftManager,
        state_machine: M,
    ) -> Result<Self> {
        let applied = state_machine.flushed_index();

        let config = Config {
            id: replica_id,
            election_tick: 3,
            heartbeat_tick: 1,
            applied,
            pre_vote: true,
            batch_append: true,
            check_quorum: true,
            max_size_per_msg: u16::MAX as _,
            ..Default::default()
        };

        let storage = Storage::open(replica_id, applied, mgr.engine.clone()).await.unwrap();
        Ok(RaftNode {
            group_id,
            lease_read_requests: Vec::default(),
            read_index_requests: Vec::default(),
            raw_node: RawNode::with_default_logger(&config, storage)?,
            applier: Applier::new(group_id, state_machine),
        })
    }

    pub fn propose(
        &mut self,
        data: Vec<u8>,
        context: Vec<u8>,
        sender: oneshot::Sender<Result<()>>,
    ) {
        if self.raw_node.raft.state != StateRole::Leader {
            sender
                .send(Err(Error::NotLeader(self.group_id, None)))
                .unwrap_or_default();
            return;
        }

        if let Err(err) = self.raw_node.propose(context, data) {
            sender.send(Err(err.into())).unwrap_or_default();
            return;
        }

        let index = self.raw_node.raft.raft_log.last_index();
        let term = self.raw_node.raft.term;
        self.applier.delegate_proposal_context(index, term, sender);
    }

    pub fn propose_conf_change(
        &mut self,
        context: Vec<u8>,
        cc: impl ConfChangeI,
        sender: oneshot::Sender<Result<()>>,
    ) {
        if self.raw_node.raft.state != StateRole::Leader {
            sender
                .send(Err(Error::NotLeader(self.group_id, None)))
                .unwrap_or_default();
            return;
        }

        if let Err(err) = self.raw_node.propose_conf_change(context, cc) {
            sender.send(Err(err.into())).unwrap_or_default();
            return;
        }

        let index = self.raw_node.raft.raft_log.last_index();
        let term = self.raw_node.raft.term;
        self.applier.delegate_proposal_context(index, term, sender);
    }

    #[inline]
    pub fn lease_read(&mut self, sender: oneshot::Sender<Result<()>>) {
        self.lease_read_requests.push(sender);
    }

    #[inline]
    pub fn read_index(&mut self, sender: oneshot::Sender<Result<()>>) {
        self.read_index_requests.push(sender);
    }

    #[inline]
    pub fn transfer_leader(&mut self, transferee: u64) {
        self.raw_node.transfer_leader(transferee);
    }

    #[inline]
    pub fn report_unreachable(&mut self, target_id: u64) {
        self.raw_node.report_unreachable(target_id);
    }

    #[inline]
    pub fn tick(&mut self) {
        self.raw_node.tick();
    }

    #[inline]
    pub fn step(&mut self, msg: Message) -> std::result::Result<(), raft::Error> {
        self.raw_node.step(msg)
    }

    fn advance_read_requests(&mut self) {
        if !self.lease_read_requests.is_empty() {
            let requests = std::mem::take(&mut self.lease_read_requests);
            self.submit_read_requests(requests);
        }

        if !self.read_index_requests.is_empty() {
            let requests = std::mem::take(&mut self.read_index_requests);
            self.submit_read_requests(requests);
        }
    }

    // FIXME(walter) support different read options.
    #[inline]
    fn submit_read_requests(&mut self, requests: Vec<oneshot::Sender<Result<()>>>) {
        let read_state_ctx = self.applier.delegate_read_requests(requests);
        self.raw_node.read_index(read_state_ctx);
    }

    /// Advance raft node, persist, apply entries and send messages.
    pub fn advance(&mut self, template: &mut impl AdvanceTemplate) -> Option<WriteTask> {
        self.advance_read_requests();
        if !self.raw_node.has_ready() {
            return None;
        }

        let mut ready = self.raw_node.ready();
        if let Some(ss) = ready.ss() {
            template.on_state_updated(ss.leader_id, self.raw_node.raft.term, ss.raft_state);
        }

        if !ready.messages().is_empty() {
            template.send_messages(ready.take_messages());
        }

        self.handle_apply(&mut ready);

        let write_task = self.build_write_task(&mut ready);
        if write_task.is_none() {
            let post_ready = PostReady::new(&mut ready);
            self.raw_node.advance_append_async(ready);
            self.post_advance(post_ready, template);
        } else {
            self.raw_node.advance_append_async(ready);
        }
        write_task
    }

    pub fn post_advance(&mut self, post_ready: PostReady, sender: &mut impl AdvanceTemplate) {
        if !post_ready.persisted_messages.is_empty() {
            sender.send_messages(post_ready.persisted_messages);
        }

        self.raw_node.on_persist_ready(post_ready.number);
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut Storage {
        self.raw_node.raft.mut_store()
    }

    fn handle_apply(&mut self, ready: &mut Ready) {
        if !ready.read_states().is_empty() {
            self.applier.apply_read_states(ready.take_read_states());
        }
        if !ready.committed_entries().is_empty() {
            self.applier.apply_entries(ready.take_committed_entries());
        }

        self.raw_node.advance_apply();
    }

    fn build_write_task(&mut self, ready: &mut Ready) -> Option<WriteTask> {
        if ready.hs().is_none() && ready.entries().is_empty() && ready.snapshot().is_empty() {
            return None;
        }

        let mut write_task = WriteTask {
            post_ready: PostReady::new(ready),
            hard_state: ready.hs().cloned(),
            entries: ready.take_entries(),
            snapshot: None,
        };

        if !ready.snapshot().is_empty() {
            write_task.snapshot = Some(ready.snapshot().clone());
        }

        Some(write_task)
    }
}

impl PostReady {
    pub fn new(ready: &mut Ready) -> Self {
        PostReady {
            number: ready.number(),
            persisted_messages: ready.take_persisted_messages(),
        }
    }
}

impl WriteTask {
    pub fn post_ready(self) -> PostReady {
        self.post_ready
    }
}
