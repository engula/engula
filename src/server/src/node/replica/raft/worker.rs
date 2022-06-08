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

use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};

use engula_api::server::v1::{ChangeReplicas, ReplicaDesc};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, SinkExt, StreamExt,
};
use raft::{prelude::*, StateRole};
use raft_engine::{Engine, LogBatch};
use tracing::{debug, warn};

use super::{
    applier::ReplicaCache,
    fsm::StateMachine,
    node::RaftNode,
    transport::{Channel, TransportManager},
    RaftManager, ReadPolicy,
};
use crate::{
    serverpb::v1::{EvalResult, RaftMessage},
    Result,
};

pub enum Request {
    Read {
        policy: ReadPolicy,
        sender: oneshot::Sender<Result<()>>,
    },
    Propose {
        eval_result: EvalResult,
        sender: oneshot::Sender<Result<()>>,
    },
    /// Try generate new snapshot.
    Snapshot {
        sender: oneshot::Sender<Result<u64>>,
    },
    ChangeConfig {
        change: ChangeReplicas,
        sender: oneshot::Sender<Result<()>>,
    },
    Transfer {
        target_id: u64,
    },
    Campaign,
    Message(RaftMessage),
    Unreachable {
        target_id: u64,
    },
    Start,
}

/// An abstraction for observing raft roles and state changes.
pub trait StateObserver: Send {
    fn on_state_updated(&mut self, leader_id: u64, term: u64, role: StateRole);
}

struct AdvanceImpl<'a> {
    group_id: u64,
    desc: ReplicaDesc,
    channels: &'a mut HashMap<u64, Channel>,
    trans_mgr: &'a TransportManager,
    observer: &'a mut Box<dyn StateObserver>,
    replica_cache: &'a mut ReplicaCache,
}

impl<'a> super::node::AdvanceTemplate for AdvanceImpl<'a> {
    fn send_messages(&mut self, msgs: Vec<Message>) {
        let mut seperated_msgs: HashMap<u64, Vec<Message>> = HashMap::default();
        for msg in msgs {
            seperated_msgs
                .entry(msg.to)
                .or_insert_with(Vec::default)
                .push(msg);
        }
        for (target_id, msgs) in seperated_msgs {
            tracing::info!("send msg to {}", target_id);
            let to_replica = match self.replica_cache.get(target_id) {
                Some(to_replica) => to_replica,
                None => {
                    warn!(
                        group = self.group_id,
                        target = target_id,
                        "send message to unknown target"
                    );
                    continue;
                }
            };
            self.channels
                .entry(target_id)
                .or_insert_with(|| Channel::new(self.trans_mgr.clone()))
                .send_message(RaftMessage {
                    group_id: self.group_id,
                    from_replica: Some(self.desc.clone()),
                    to_replica: Some(to_replica),
                    message: msgs,
                });
        }
    }

    fn on_state_updated(&mut self, leader_id: u64, term: u64, role: raft::StateRole) {
        self.observer.on_state_updated(leader_id, term, role);
    }

    fn mut_replica_cache(&mut self) -> &mut ReplicaCache {
        self.replica_cache
    }
}

/// A structure wraps raft node execution logics.
pub struct RaftWorker<M: StateMachine>
where
    Self: Send,
{
    request_sender: mpsc::Sender<Request>,
    request_receiver: mpsc::Receiver<Request>,

    group_id: u64,
    desc: ReplicaDesc,
    raft_node: RaftNode<M>,

    channels: HashMap<u64, Channel>,
    trans_mgr: TransportManager,
    engine: Arc<Engine>,
    observer: Box<dyn StateObserver>,
    replica_cache: ReplicaCache,

    marker: PhantomData<M>,
}

// Send is safe because the ReplicaCache field is not accessible outside of RaftWorker.
unsafe impl<M: StateMachine> Send for RaftWorker<M> {}

impl<M> RaftWorker<M>
where
    M: StateMachine,
{
    pub async fn open(
        group_id: u64,
        desc: ReplicaDesc,
        state_machine: M,
        raft_mgr: &RaftManager,
        observer: Box<dyn StateObserver>,
    ) -> Result<Self> {
        let mut replica_cache = ReplicaCache::default();
        replica_cache.insert(desc.clone());
        replica_cache.batch_insert(&state_machine.descriptor().replicas);
        let raft_node = RaftNode::new(group_id, desc.id, raft_mgr, state_machine).await?;

        // TODO(walter) config channel size.
        let (mut request_sender, request_receiver) = mpsc::channel(10240);
        request_sender.send(Request::Start).await.unwrap();

        Ok(RaftWorker {
            request_sender,
            request_receiver,
            group_id,
            desc,
            raft_node,
            channels: HashMap::new(),
            trans_mgr: raft_mgr.transport_mgr.clone(),
            engine: raft_mgr.engine.clone(),
            observer,
            replica_cache,
            marker: PhantomData,
        })
    }

    #[inline]
    pub fn request_sender(&self) -> mpsc::Sender<Request> {
        self.request_sender.clone()
    }

    /// Poll requests and messages, forward both to `RaftNode`, and advance `RaftNode`.
    pub async fn run(mut self) -> Result<()> {
        debug!(
            "raft worker of replica {} group {} start running",
            self.desc.id, self.group_id
        );
        // WARNING: the underlying instant isn't steady.
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            if !self.raft_node.has_ready() {
                futures::select_biased! {
                    _ = interval.tick().fuse() => {
                        self.raft_node.tick();
                    },
                    request = self.request_receiver.next() => match request {
                        Some(request) => {
                            self.handle_request(request)?;
                        },
                        None => break,
                    },
                }
            }

            while let Ok(Some(request)) = self.request_receiver.try_next() {
                self.handle_request(request)?;
            }

            let mut template = AdvanceImpl {
                group_id: self.group_id,
                desc: self.desc.clone(),
                channels: &mut self.channels,
                trans_mgr: &self.trans_mgr,
                observer: &mut self.observer,
                replica_cache: &mut self.replica_cache,
            };
            if let Some(write_task) = self.raft_node.advance(&mut template) {
                let mut batch = LogBatch::default();
                self.raft_node
                    .mut_store()
                    .write(&mut batch, &write_task)
                    .expect("write log batch");
                self.engine.write(&mut batch, false).unwrap();
                let post_ready = write_task.post_ready();
                self.raft_node.post_advance(post_ready, &mut template);
            }
        }

        todo!("handle exit");
    }

    fn handle_request(&mut self, request: Request) -> Result<()> {
        match request {
            Request::Propose {
                eval_result,
                sender,
            } => self.handle_proposal(eval_result, sender),
            Request::Read { policy, sender } => self.handle_read(policy, sender),
            Request::ChangeConfig { change, sender } => self.handle_conf_change(change, sender),
            Request::Snapshot { .. } => {
                todo!()
            }
            Request::Transfer { target_id } => {
                self.raft_node.transfer_leader(target_id);
            }
            Request::Campaign => {
                todo!()
            }
            Request::Message(msg) => {
                self.handle_msg(msg).unwrap();
            }
            Request::Unreachable { target_id } => {
                self.raft_node.report_unreachable(target_id);
            }
            Request::Start => {}
        }
        Ok(())
    }

    fn handle_msg(&mut self, msg: RaftMessage) -> Result<()> {
        if let Some(from) = msg.from_replica {
            self.replica_cache.insert(from);
        }
        for msg in msg.message {
            self.raft_node.step(msg)?;
        }
        Ok(())
    }

    fn handle_proposal(&mut self, eval_result: EvalResult, sender: oneshot::Sender<Result<()>>) {
        use prost::Message;

        let data = eval_result.encode_to_vec();
        self.raft_node.propose(data, vec![], sender);
    }

    fn handle_conf_change(&mut self, change: ChangeReplicas, sender: oneshot::Sender<Result<()>>) {
        let cc = super::encode_to_conf_change(change);
        self.raft_node.propose_conf_change(vec![], cc, sender);
    }

    fn handle_read(&mut self, policy: ReadPolicy, sender: oneshot::Sender<Result<()>>) {
        match policy {
            ReadPolicy::Relaxed => {
                panic!("not support");
            }
            ReadPolicy::LeaseRead => {
                self.raft_node.lease_read(sender);
            }
            ReadPolicy::ReadIndex => {
                self.raft_node.read_index(sender);
            }
        }
    }
}
