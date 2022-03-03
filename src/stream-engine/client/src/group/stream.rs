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
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use futures::channel::oneshot;
use stream_engine_proto::{ObserverState, SegmentDesc};
use tokio::sync::mpsc::UnboundedSender;

use super::timer::TimerHandle;
use crate::{
    core::{Learn, Message, Mutate, StreamStateMachine},
    master::Stream as MasterStream,
    EpochState, Error, Result, Role, Sequence,
};

#[derive(Debug)]
pub(crate) struct Promote {
    pub role: Role,
    pub epoch: u32,
    pub leader: String,
    pub copy_set: Vec<String>,
    pub broken_segments: Vec<SegmentDesc>,
}

#[derive(Derivative)]
#[derivative(Debug)]
enum StreamEvent {
    Tick,
    Promote(Box<Promote>),
    Msg(Message),
    Proposal {
        #[derivative(Debug = "ignore")]
        event: Box<[u8]>,
        #[derivative(Debug = "ignore")]
        sender: oneshot::Sender<Result<Sequence>>,
    },
    Truncate {
        up_to: Sequence,
        #[derivative(Debug = "ignore")]
        sender: oneshot::Sender<Result<()>>,
    },
    EpochState {
        #[derivative(Debug = "ignore")]
        sender: oneshot::Sender<EpochState>,
    },
}

type StateObserver = UnboundedSender<EpochState>;

pub(crate) struct StreamMixin<L> {
    pub channel: EventChannel<L>,
    pub master_stream: MasterStream,

    state_machine: StreamStateMachine,
    state_observer: Option<StateObserver>,
    applier: Applier,
}

impl<L: ActiveLauncher> StreamMixin<L> {
    pub fn new(channel: EventChannel<L>, master_stream: MasterStream) -> Self {
        let stream_id = master_stream.stream_id();
        StreamMixin {
            channel,
            master_stream,
            state_machine: StreamStateMachine::new(stream_id),
            state_observer: None,
            applier: Applier::new(),
        }
    }

    fn handle_proposal(&mut self, event: Box<[u8]>, sender: oneshot::Sender<Result<Sequence>>) {
        match self.state_machine.propose(event) {
            Ok(seq) => self.applier.push_proposal(seq, sender),
            Err(err) => sender.send(Err(err)).unwrap_or_default(),
        }
    }

    fn handle_truncate(&mut self, sequence: u64, sender: oneshot::Sender<Result<()>>) {
        if self.state_machine.role != Role::Follower {
            sender
                .send(Err(Error::NotLeader(self.state_machine.leader.clone())))
                .unwrap_or_default();
            return;
        }

        let acked_seq: u64 = self.state_machine.acked_seq().into();
        if acked_seq.checked_sub(sequence).is_none() {
            sender
                .send(Err(Error::InvalidArgument(format!(
                    "truncate sequence (is {}) should be <= end (is {})",
                    sequence, acked_seq
                ))))
                .unwrap_or_default();
            return;
        }

        // TODO(w41ter) truncate entries from master and state machine.
        sender.send(Ok(())).unwrap_or_default();
    }

    fn handle_promote(&mut self, promote: Promote) {
        self.state_machine.promote(
            promote.epoch,
            promote.role,
            promote.leader,
            promote.copy_set,
            promote
                .broken_segments
                .into_iter()
                .map(|m| (m.epoch, m.copy_set))
                .collect(),
        );

        // FIXME(w41ter) cancel subscribing when the receiving end are closed.
        if let Some(state_observer) = &mut self.state_observer {
            state_observer
                .send(self.state_machine.epoch_state())
                .unwrap_or_default();
        }
    }

    fn dispatch_events(&mut self) -> bool {
        let mut heartbeat = false;
        for event in self.channel.take_events() {
            match event {
                StreamEvent::Msg(msg) => {
                    self.state_machine.step(msg);
                }
                StreamEvent::Proposal { event, sender } => {
                    self.handle_proposal(event, sender);
                }
                StreamEvent::Truncate { up_to, sender } => {
                    self.handle_truncate(up_to.into(), sender);
                }
                StreamEvent::Tick => {
                    self.state_machine.tick();
                    heartbeat = true;
                }
                StreamEvent::Promote(promote) => {
                    self.handle_promote(*promote);
                }
                StreamEvent::EpochState { sender } => {
                    sender
                        .send(self.state_machine.epoch_state())
                        .unwrap_or_default();
                }
            }
        }
        heartbeat
    }

    fn flush_ready(&mut self, scheduler: &mut impl Scheduler) {
        let ready = match self.state_machine.collect() {
            Some(ready) => ready,
            None => return,
        };

        self.applier.might_advance(ready.acked_seq);
        scheduler.handle_writes(ready.pending_writes);
        scheduler.handle_learns(ready.pending_learns);
        if let Some(restored) = ready.restored_segment {
            scheduler.seal_segment(restored.segment_epoch, restored.writer_epoch);
        }
    }

    pub fn observe(&mut self, state_observer: StateObserver) -> Result<()> {
        if self.state_observer.is_some() {
            return Err(Error::AlreadyExists("state observer".to_owned()));
        }
        self.state_observer = Some(state_observer);
        Ok(())
    }

    pub fn on_active(&mut self, scheduler: &mut impl Scheduler) {
        if self.dispatch_events() {
            // If state observer is none, the user don't subscribe the change of epoch
            // states, and the stream shouldn't participate in election.
            if self.state_observer.is_some() {
                scheduler.send_heartbeat(
                    self.state_machine.role,
                    self.state_machine.writer_epoch,
                    self.state_machine.acked_seq(),
                    self.state_machine.observer_state(),
                );
            }
        }
        self.flush_ready(scheduler);
    }
}

pub(crate) struct Applier {
    proposals: VecDeque<(Sequence, oneshot::Sender<Result<Sequence>>)>,
}

impl Applier {
    fn new() -> Self {
        Applier {
            proposals: VecDeque::new(),
        }
    }

    #[inline(always)]
    fn push_proposal(&mut self, seq: Sequence, sender: oneshot::Sender<Result<Sequence>>) {
        self.proposals.push_back((seq, sender));
    }

    /// Notify acked proposals.
    fn might_advance(&mut self, acked_seq: Sequence) {
        while let Some((seq, _)) = self.proposals.front() {
            if *seq > acked_seq {
                break;
            }

            let (seq, sender) = self.proposals.pop_front().unwrap();
            sender
                .send(Ok(seq))
                // The receiving end was canceled.
                .unwrap_or(());
        }
    }
}

/// FIXME(w41ter) support back pressure.
struct ChannelState<L> {
    events: VecDeque<StreamEvent>,
    launcher: Option<L>,
}

pub(crate) struct EventChannel<L> {
    stream_id: u64,
    state: Arc<Mutex<ChannelState<L>>>,
}

impl<L: ActiveLauncher> EventChannel<L> {
    pub fn new(stream_id: u64) -> Self {
        EventChannel {
            stream_id,
            state: Arc::new(Mutex::new(ChannelState {
                events: VecDeque::new(),
                launcher: None,
            })),
        }
    }

    #[allow(dead_code)]
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn take_events(&self) -> Vec<StreamEvent> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.events).into_iter().collect()
    }

    pub(super) fn poll(&self, launcher: L) {
        let mut state = self.state.lock().unwrap();
        if state.events.is_empty() {
            state.launcher = Some(launcher);
        } else {
            drop(state);

            launcher.fire(self.stream_id);
        }
    }

    fn send(&self, event: StreamEvent) {
        if let Some(launcher) = {
            let mut state = self.state.lock().unwrap();

            state.events.push_back(event);
            state.launcher.take()
        } {
            launcher.fire(self.stream_id);
        };
    }

    #[inline(always)]
    pub fn on_tick(&self) {
        self.send(StreamEvent::Tick);
    }

    #[inline(always)]
    pub fn on_propose(&self, event: Box<[u8]>) -> oneshot::Receiver<Result<Sequence>> {
        let (sender, receiver) = oneshot::channel();
        self.send(StreamEvent::Proposal { event, sender });
        receiver
    }

    #[inline(always)]
    pub fn on_truncate(&self, up_to: Sequence) -> oneshot::Receiver<Result<()>> {
        let (sender, receiver) = oneshot::channel();
        self.send(StreamEvent::Truncate { up_to, sender });
        receiver
    }

    #[inline(always)]
    pub fn on_msg(&self, msg: Message) {
        self.send(StreamEvent::Msg(msg));
    }

    #[inline(always)]
    pub fn on_epoch_state(&self) -> oneshot::Receiver<EpochState> {
        let (sender, receiver) = oneshot::channel();
        self.send(StreamEvent::EpochState { sender });
        receiver
    }

    #[inline(always)]
    pub fn on_promote(&self, promote: Box<Promote>) {
        self.send(StreamEvent::Promote(promote));
    }
}

impl<L: ActiveLauncher> TimerHandle for EventChannel<L> {
    fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn on_fire(&self) {
        self.on_tick();
    }
}

// Bypass issue https://github.com/rust-lang/rust/issues/26925.
impl<L> Clone for EventChannel<L> {
    fn clone(&self) -> Self {
        EventChannel {
            stream_id: self.stream_id,
            state: self.state.clone(),
        }
    }
}

pub(crate) trait ActiveLauncher {
    fn fire(&self, stream_id: u64);
}

pub(crate) trait Scheduler {
    fn send_heartbeat(
        &mut self,
        role: Role,
        writer_epoch: u32,
        acked_seq: Sequence,
        observer_state: ObserverState,
    );

    fn seal_segment(&mut self, segment_epoch: u32, writer_epoch: u32);

    fn handle_writes(&mut self, writes: Vec<Mutate>);

    fn handle_learns(&mut self, learns: Vec<Learn>);
}
