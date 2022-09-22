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

use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, Instant},
};

use engula_api::server::v1::{ChangeReplicas, RaftRole, ReplicaDesc};
use futures::{
    channel::{mpsc, oneshot},
    stream::FusedStream,
    FutureExt, SinkExt, StreamExt,
};
use raft::{prelude::*, SoftState, StateRole};
use raft_engine::{Engine, LogBatch};
use tracing::{debug, warn};

use super::{
    applier::{Applier, ReplicaCache},
    fsm::StateMachine,
    metrics::*,
    monitor::WorkerPerfContext,
    node::RaftNode,
    snap::{apply::apply_snapshot, RecycleSnapMode, SnapManager},
    transport::{Channel, TransportManager},
    RaftManager, ReadPolicy,
};
use crate::{
    raftgroup::monitor::record_perf_point,
    record_latency,
    runtime::Executor,
    serverpb::v1::{EvalResult, RaftMessage},
    RaftConfig, Result,
};

pub enum Request {
    Read {
        policy: ReadPolicy,
        sender: oneshot::Sender<Result<()>>,
    },
    Propose {
        eval_result: EvalResult,
        start: Instant,
        sender: oneshot::Sender<Result<()>>,
    },
    CreateSnapshotFinished,
    InstallSnapshot {
        msg: Message,
    },
    RejectSnapshot {
        msg: Message,
    },
    ChangeConfig {
        change: ChangeReplicas,
        sender: oneshot::Sender<Result<()>>,
    },
    Transfer {
        transferee: u64,
    },
    Message(RaftMessage),
    Unreachable {
        target_id: u64,
    },
    State(oneshot::Sender<RaftGroupState>),
    Monitor(oneshot::Sender<Box<WorkerPerfContext>>),
    Start,
}

pub struct PeerState {
    /// How much state is matched.
    pub matched: u64,
    /// The next index to apply
    pub next_idx: u64,

    /// Committed index in raft_log
    pub committed_index: u64,

    pub might_lost: bool,
}

#[derive(Default)]
pub struct RaftGroupState {
    /// The hardstate of the raft, representing voted state.
    pub hs: HardState,
    /// The softstate of the raft, representing proposed state.
    pub ss: SoftState,
    /// The index of the last entry which has been applied.
    pub applied: u64,
    /// The index of the last entry which has been committed.
    pub committed: u64,
    /// The first index of log entries.
    pub first_index: u64,
    /// The last index of log entries.
    pub last_index: u64,

    pub peers: HashMap<u64, PeerState>,
}

/// An abstraction for observing raft roles and state changes.
pub trait StateObserver: Send {
    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole);
}

struct SlowIoGuard {
    threshold: u64,
    start: Instant,
}

struct AdvanceImpl<'a> {
    group_id: u64,
    replica_id: u64,
    desc: ReplicaDesc,
    channels: &'a mut HashMap<u64, Channel>,
    trans_mgr: &'a TransportManager,
    snap_mgr: &'a SnapManager,
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
                    messages: msgs,
                });
        }
    }

    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole) {
        self.observer
            .on_state_updated(leader_id, voted_for, term, role);
    }

    fn mut_replica_cache(&mut self) -> &mut ReplicaCache {
        self.replica_cache
    }

    #[inline]
    fn apply_snapshot<M: StateMachine>(&mut self, applier: &mut Applier<M>, snapshot: &Snapshot) {
        apply_snapshot(self.replica_id, self.snap_mgr, applier, snapshot);
    }
}

/// A structure wraps raft node execution logics.
pub struct RaftWorker<M: StateMachine>
where
    Self: Send,
{
    cfg: RaftConfig,
    executor: Executor,
    request_sender: mpsc::Sender<Request>,
    request_receiver: mpsc::Receiver<Request>,

    group_id: u64,
    desc: ReplicaDesc,
    raft_node: RaftNode<M>,

    channels: HashMap<u64, Channel>,
    trans_mgr: TransportManager,
    snap_mgr: SnapManager,
    engine: Arc<Engine>,
    observer: Box<dyn StateObserver>,
    replica_cache: ReplicaCache,

    marker: PhantomData<M>,
}

#[derive(Default)]
struct WorkerContext {
    accumulated_bytes: usize,
    perf_ctx: WorkerPerfContext,
    monitors: Vec<oneshot::Sender<Box<WorkerPerfContext>>>,
}

impl<M> RaftWorker<M>
where
    M: StateMachine,
{
    pub async fn open(
        group_id: u64,
        replica_id: u64,
        node_id: u64,
        state_machine: M,
        raft_mgr: &RaftManager,
        mut observer: Box<dyn StateObserver>,
    ) -> Result<Self> {
        let desc = ReplicaDesc {
            id: replica_id,
            node_id,
            ..Default::default()
        };
        let mut replica_cache = ReplicaCache::default();
        replica_cache.insert(desc.clone());
        replica_cache.batch_insert(&state_machine.descriptor().replicas);
        let raft_node = RaftNode::new(group_id, replica_id, raft_mgr, state_machine).await?;

        let (mut request_sender, request_receiver) =
            mpsc::channel(raft_mgr.cfg.max_inflight_requests);
        request_sender.send(Request::Start).await.unwrap();

        observer.on_state_updated(
            raft_node.raft().leader_id,
            raft_node.raft().vote,
            raft_node.raft().term,
            RaftRole::Follower,
        );

        Ok(RaftWorker {
            cfg: raft_mgr.cfg.clone(),
            executor: raft_mgr.executor().clone(),
            request_sender,
            request_receiver,
            group_id,
            desc,
            raft_node,
            channels: HashMap::new(),
            trans_mgr: raft_mgr.transport_mgr.clone(),
            snap_mgr: raft_mgr.snap_mgr.clone(),
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
            "group {} replica {} raft worker is running",
            self.group_id, self.desc.id
        );

        // WARNING: the underlying instant isn't steady.
        let mut interval = tokio::time::interval(Duration::from_millis(self.cfg.tick_interval_ms));
        while !self.request_receiver.is_terminated() {
            let mut ctx = WorkerContext::default();
            self.maintenance(&mut ctx, &mut interval).await?;
            self.consume_requests(&mut ctx)?;
            self.dispatch(&mut ctx)?;
            self.finish_round(ctx);
            crate::runtime::yield_now().await;
        }

        debug!(
            "group {} replica {} raft worker is quit",
            self.group_id, self.desc.id
        );

        Ok(())
    }

    async fn maintenance(
        &mut self,
        ctx: &mut WorkerContext,
        interval: &mut tokio::time::Interval,
    ) -> Result<()> {
        if !self.raft_node.has_ready() {
            futures::select_biased! {
                _ = interval.tick().fuse() => {
                    self.raft_node.tick();
                    self.compact_log(ctx);
                },
                request = self.request_receiver.next() => if let Some(req) = request {
                    self.handle_request(ctx, req)?;
                },
            }
            record_perf_point(&mut ctx.perf_ctx.wake);
        }
        Ok(())
    }

    fn consume_requests(&mut self, ctx: &mut WorkerContext) -> Result<()> {
        record_latency!(&RAFTGROUP_WORKER_CONSUME_REQUESTS_DURATION_SECONDS);
        record_perf_point(&mut ctx.perf_ctx.consume_requests);
        while let Ok(Some(request)) = self.request_receiver.try_next() {
            self.handle_request(ctx, request)?;
            if ctx.accumulated_bytes >= self.cfg.max_io_batch_size as usize {
                break;
            }
        }
        Ok(())
    }

    fn dispatch(&mut self, ctx: &mut WorkerContext) -> Result<()> {
        RAFTGROUP_WORKER_ACCUMULATED_BYTES_SIZE.observe(ctx.accumulated_bytes as f64);
        RAFTGROUP_WORKER_ADVANCE_TOTAL.inc();
        record_latency!(&RAFTGROUP_WORKER_ADVANCE_DURATION_SECONDS);
        let mut template = AdvanceImpl {
            replica_id: self.desc.id,
            group_id: self.group_id,
            desc: self.desc.clone(),
            channels: &mut self.channels,
            trans_mgr: &self.trans_mgr,
            snap_mgr: &self.snap_mgr,
            observer: &mut self.observer,
            replica_cache: &mut self.replica_cache,
        };
        if let Some(write_task) = self
            .raft_node
            .advance(&mut ctx.perf_ctx.advance, &mut template)
        {
            let mut batch = LogBatch::default();
            self.raft_node
                .mut_store()
                .write(&mut batch, &write_task)
                .expect("write log batch");

            let _slow_io_guard = self.cfg.engine_slow_io_threshold_ms.map(SlowIoGuard::new);
            record_perf_point(&mut ctx.perf_ctx.write);
            ctx.perf_ctx.num_writes = write_task.entries.len();
            self.engine.write(&mut batch, false).unwrap();
            let post_ready = write_task.post_ready();
            self.raft_node
                .post_advance(&mut ctx.perf_ctx.advance, post_ready, &mut template);
        }

        if self.raft_node.mut_store().create_snapshot.get() {
            self.raft_node.mut_store().create_snapshot.set(false);
            super::snap::dispatch_creating_snap_task(
                &self.executor,
                self.desc.id,
                self.request_sender.clone(),
                self.raft_node.mut_state_machine(),
                self.snap_mgr.clone(),
            );
        }

        Ok(())
    }

    fn finish_round(&self, mut ctx: WorkerContext) {
        record_perf_point(&mut ctx.perf_ctx.finish);
        ctx.perf_ctx.accumulated_bytes = ctx.accumulated_bytes;
        for sender in ctx.monitors {
            sender
                .send(Box::new(ctx.perf_ctx.clone()))
                .unwrap_or_default();
        }
    }

    fn handle_request(&mut self, ctx: &mut WorkerContext, request: Request) -> Result<()> {
        ctx.perf_ctx.num_requests += 1;
        match request {
            Request::Propose {
                eval_result,
                start,
                sender,
            } => self.handle_proposal(ctx, eval_result, start, sender),
            Request::Read { policy, sender } => self.handle_read(policy, sender),
            Request::ChangeConfig { change, sender } => self.handle_conf_change(change, sender),
            Request::CreateSnapshotFinished => {
                self.raft_node.mut_store().is_creating_snapshot.set(false);
            }
            Request::Transfer {
                transferee: target_id,
            } => {
                self.raft_node.transfer_leader(target_id);
            }
            Request::Message(msg) => {
                self.handle_msg(ctx, msg)?;
            }
            Request::Unreachable { target_id } => {
                self.raft_node.report_unreachable(target_id);
            }
            Request::RejectSnapshot { msg: input } => {
                let mut msg = Message::default();
                msg.set_msg_type(MessageType::MsgSnapStatus);
                msg.from = input.to;
                msg.to = input.from;
                msg.reject = true;

                if let Some(to_replica) = self.replica_cache.get(input.from) {
                    self.channels
                        .entry(input.from)
                        .or_insert_with(|| Channel::new(self.trans_mgr.clone()))
                        .send_message(RaftMessage {
                            group_id: self.group_id,
                            from_replica: Some(self.desc.clone()),
                            to_replica: Some(to_replica),
                            messages: vec![msg],
                        });
                }
            }
            Request::InstallSnapshot { msg } => {
                self.raft_node.step(msg)?;
            }
            Request::State(sender) => {
                let store = self.raft_node.mut_store();
                let first_index = store.first_index().unwrap();
                let last_index = store.last_index().unwrap();
                sender
                    .send(self.raft_group_state(first_index, last_index))
                    .unwrap_or_default();
            }
            Request::Monitor(sender) => {
                ctx.monitors.push(sender);
            }
            Request::Start => {}
        }
        Ok(())
    }

    fn handle_msg(&mut self, ctx: &mut WorkerContext, raft_msg: RaftMessage) -> Result<()> {
        let from_replica = raft_msg.from_replica.unwrap();
        self.replica_cache.insert(from_replica.clone());
        for msg in raft_msg.messages {
            if msg.get_msg_type() == MessageType::MsgSnapshot {
                // TODO(walter) In order to avoid useless downloads, should check whether this
                // snapshot will be accept.
                super::snap::dispatch_downloading_snap_task(
                    &self.executor,
                    self.desc.id,
                    self.request_sender.clone(),
                    self.snap_mgr.clone(),
                    self.trans_mgr.clone(),
                    from_replica.clone(),
                    msg,
                );
            } else {
                ctx.accumulated_bytes += msg.entries.iter().map(|e| e.data.len()).sum::<usize>();
                ctx.perf_ctx.num_step_msg += 1;
                self.raft_node.step(msg)?;
            }
        }
        Ok(())
    }

    fn handle_proposal(
        &mut self,
        ctx: &mut WorkerContext,
        eval_result: EvalResult,
        start: Instant,
        sender: oneshot::Sender<Result<()>>,
    ) {
        use prost::Message;

        let data = eval_result.encode_to_vec();
        ctx.accumulated_bytes += data.len();
        ctx.perf_ctx.num_proposal += 1;
        self.raft_node.propose(data, vec![], sender);
        RAFTGROUP_WORKER_REQUEST_IN_QUEUE_DURATION_SECONDS.observe(elapsed_seconds(start));
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

    fn compact_log(&mut self, ctx: &mut WorkerContext) {
        record_latency!(&RAFTGROUP_WORKER_COMPACT_LOG_DURATION_SECONDS);
        record_perf_point(&mut ctx.perf_ctx.compact_log);
        let mut to = self.raft_node.mut_state_machine().flushed_index();

        let status = self.raft_node.raft_status();
        if status.ss.raft_state == StateRole::Leader {
            if let Some(min_matched_index) = status
                .progress
                .and_then(|p| p.iter().map(|(_, p)| p.matched).min())
            {
                to = std::cmp::min(min_matched_index, to);
            }
        }

        let store = self.raft_node.mut_store();
        if store.first_index().unwrap() < to {
            let mut lb = store.compact_to(to);
            self.engine.write(&mut lb, false).unwrap();
        }

        self.snap_mgr
            .recycle_snapshots(self.desc.id, RecycleSnapMode::RequiredIndex(to));
    }

    fn raft_group_state(&self, first_index: u64, last_index: u64) -> RaftGroupState {
        let status = self.raft_node.raft_status();

        let mut peer_states = HashMap::new();
        if let Some(tracker) = status.progress {
            for (id, progress) in tracker.iter() {
                let state = PeerState {
                    matched: progress.matched,
                    next_idx: progress.next_idx,
                    committed_index: progress.next_idx,
                    might_lost: progress.might_lost,
                };
                peer_states.insert(*id, state);
            }
        }

        RaftGroupState {
            hs: status.hs,
            ss: status.ss,
            applied: status.applied,
            committed: self.raft_node.committed_index(),
            first_index,
            last_index,
            peers: peer_states,
        }
    }
}

impl SlowIoGuard {
    fn new(threshold: u64) -> Self {
        SlowIoGuard {
            threshold,
            start: Instant::now(),
        }
    }
}

impl Drop for SlowIoGuard {
    fn drop(&mut self) {
        if self.start.elapsed() >= Duration::from_millis(self.threshold) {
            warn!("raft engine slow io: {:?}", raft_engine::get_perf_context());
        }
    }
}
