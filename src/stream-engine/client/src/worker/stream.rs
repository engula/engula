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

use stream_engine_proto::{ObserverState, SegmentDesc};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use super::timer::TimerHandle;
use crate::{
    core::{Learn, Message, Mutate, StreamStateMachine},
    EpochState, Result, Role, Sequence,
};

#[derive(Debug)]
pub(crate) struct Promote {
    role: Role,
    epoch: u32,
    leader: String,
    copy_set: Vec<String>,
    broken_segments: Vec<SegmentDesc>,
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
    EpochState {
        #[derivative(Debug = "ignore")]
        sender: oneshot::Sender<EpochState>,
    },
}

type StateObserver = UnboundedSender<EpochState>;

pub(crate) struct StreamMixin<L> {
    pub channel: EventChannel<L>,
    state_machine: StreamStateMachine,
    state_observer: StateObserver,
    applier: Applier,
}

impl<L: ActiveLauncher> StreamMixin<L> {
    fn handle_proposal(&mut self, event: Box<[u8]>, sender: oneshot::Sender<Result<Sequence>>) {
        match self.state_machine.propose(event) {
            Ok(seq) => self.applier.push_proposal(seq, sender),
            Err(err) => sender.send(Err(err)).unwrap_or_default(),
        }
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
        self.state_observer
            .send(self.state_machine.epoch_state())
            .unwrap_or_default();
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

    #[allow(dead_code)]
    pub fn on_active(&mut self, scheduler: &mut impl Scheduler) {
        if self.dispatch_events() {
            scheduler.send_heartbeat(
                self.state_machine.role,
                self.state_machine.writer_epoch,
                self.state_machine.acked_seq(),
                self.state_machine.observer_state(),
            );
        }
        self.flush_ready(scheduler);
    }
}

pub(crate) struct Applier {
    proposals: VecDeque<(Sequence, oneshot::Sender<Result<Sequence>>)>,
}

impl Applier {
    #[allow(dead_code)]
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

#[allow(dead_code)]
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

    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn take_events(&self) -> Vec<StreamEvent> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.events).into_iter().collect()
    }

    fn poll(&self, launcher: L) {
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
    pub fn on_propose(&self, event: Box<[u8]>, sender: oneshot::Sender<Result<Sequence>>) {
        self.send(StreamEvent::Proposal { event, sender });
    }

    #[inline(always)]
    pub fn on_msg(&self, msg: Message) {
        self.send(StreamEvent::Msg(msg));
    }

    #[inline(always)]
    pub fn on_epoch_state(&self, sender: oneshot::Sender<EpochState>) {
        self.send(StreamEvent::EpochState { sender });
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
