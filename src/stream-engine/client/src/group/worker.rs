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
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};

use tokio::{
    runtime::Handle as RuntimeHandle,
    sync::{mpsc::UnboundedSender, oneshot},
};

use super::{
    io::{IoContext, IoScheduler},
    stream::{ActiveLauncher, EventChannel, StreamMixin},
    timer::MonoTimer,
};
use crate::{master::Stream as MasterStream, store::Transport, EpochState, Error, Result};

#[derive(Clone)]
pub(crate) struct Launcher {
    selector: Selector,
}

impl ActiveLauncher for Launcher {
    fn fire(&self, stream_id: u64) {
        let (lock, cond) = &*self.selector.state;

        let mut state = lock.lock().unwrap();
        state.actives.insert(stream_id);
        if state.wait {
            state.wait = false;
            cond.notify_one();
        }
    }
}

enum Action {
    Add {
        channel: EventChannel<Launcher>,
        master_stream: MasterStream,
        state_observer: UnboundedSender<EpochState>,
        sender: oneshot::Sender<Result<()>>,
    },
    Remove {
        stream_id: u64,
        sender: oneshot::Sender<Result<()>>,
    },
}

struct ActionChannelState {
    actions: Vec<Action>,
    launcher: Option<Launcher>,
}

#[derive(Clone)]
pub(crate) struct ActionChannel {
    state: Arc<Mutex<ActionChannelState>>,
}

impl ActionChannel {
    pub fn new() -> Self {
        ActionChannel {
            state: Arc::new(Mutex::new(ActionChannelState {
                actions: Vec::new(),
                launcher: None,
            })),
        }
    }

    fn take_actions(&self) -> Vec<Action> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.actions)
    }

    fn poll(&self, launcher: Launcher) {
        let mut state = self.state.lock().unwrap();
        if state.actions.is_empty() {
            state.launcher = Some(launcher);
        } else {
            drop(state);

            launcher.fire(u64::MAX);
        }
    }

    fn submit(&self, act: Action) {
        if let Some(launcher) = {
            let mut state = self.state.lock().unwrap();
            state.actions.push(act);
            state.launcher.take()
        } {
            launcher.fire(u64::MAX);
        }
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn add(
        &self,
        channel: EventChannel<Launcher>,
        master_stream: MasterStream,
        state_observer: UnboundedSender<EpochState>,
    ) -> oneshot::Receiver<Result<()>> {
        let (sender, receiver) = oneshot::channel();
        self.submit(Action::Add {
            channel,
            master_stream,
            state_observer,
            sender,
        });
        receiver
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn remove(&self, stream_id: u64) -> oneshot::Receiver<Result<()>> {
        let (sender, receiver) = oneshot::channel();
        self.submit(Action::Remove { stream_id, sender });
        receiver
    }
}

struct SelectorState {
    wait: bool,
    actives: HashSet<u64>,
}

#[derive(Clone)]
pub(crate) struct Selector {
    state: Arc<(Mutex<SelectorState>, Condvar)>,
}

impl Selector {
    pub fn new() -> Self {
        Selector {
            state: Arc::new((
                Mutex::new(SelectorState {
                    wait: false,
                    actives: HashSet::new(),
                }),
                Condvar::new(),
            )),
        }
    }

    #[inline(always)]
    fn launcher(&self) -> Launcher {
        Launcher {
            selector: self.clone(),
        }
    }

    fn select(
        &self,
        act_channel: Option<ActionChannel>,
        consumed: &[EventChannel<Launcher>],
    ) -> HashSet<u64> {
        if let Some(channel) = act_channel {
            channel.poll(self.launcher());
        }
        for channel in consumed {
            channel.poll(self.launcher());
        }

        let (lock, cond) = &*self.state;
        let mut state = lock.lock().unwrap();
        while state.actives.is_empty() {
            state.wait = true;
            state = cond.wait(state).unwrap();
        }

        std::mem::take(&mut state.actives)
    }
}

#[allow(dead_code)]
pub(crate) struct WorkerOption {
    pub observer_id: String,
    pub heartbeat_interval_ms: u64,

    pub runtime_handle: RuntimeHandle,
}

#[allow(dead_code)]
pub(crate) struct Worker {
    io_ctx: Arc<IoContext>,
    selector: Selector,
    mono_timer: MonoTimer<EventChannel<Launcher>>,

    channel: ActionChannel,
    streams: HashMap<u64, StreamMixin<Launcher>>,
}

#[allow(dead_code)]
impl Worker {
    pub fn new(opt: WorkerOption) -> Self {
        let io_ctx = IoContext {
            observer_id: opt.observer_id,
            runtime: opt.runtime_handle,
            transport: Transport::new(),
        };
        Worker {
            io_ctx: Arc::new(io_ctx),
            selector: Selector::new(),
            mono_timer: MonoTimer::new(opt.heartbeat_interval_ms),

            channel: ActionChannel::new(),
            streams: HashMap::new(),
        }
    }

    pub fn action_channel(&self) -> ActionChannel {
        self.channel.clone()
    }

    fn do_actions(&mut self, consumed: &mut Vec<EventChannel<Launcher>>) {
        for action in self.channel.take_actions() {
            match action {
                Action::Add {
                    channel,
                    master_stream,
                    state_observer,
                    sender,
                } => {
                    let stream_id = master_stream.stream_id();
                    let cloned_channel = channel.clone();
                    let stream = StreamMixin::new(channel, master_stream, state_observer);
                    if self.streams.insert(stream_id, stream).is_some() {
                        sender
                            .send(Err(Error::AlreadyExists(format!("stream {}", stream_id))))
                            .unwrap_or_default();
                    } else {
                        self.mono_timer.register(cloned_channel.clone());
                        consumed.push(cloned_channel);
                        sender.send(Ok(())).unwrap_or_default();
                    }
                }
                Action::Remove { stream_id, sender } => {
                    // FIXME(w41ter) need transfer leadership!
                    if let Some(_stream) = self.streams.remove(&stream_id) {
                        self.mono_timer.unregister(stream_id);
                        sender.send(Ok(())).unwrap_or_default();
                    } else {
                        sender
                            .send(Err(Error::NotFound(format!("stream {}", stream_id))))
                            .unwrap_or_default();
                    }
                }
            }
        }
    }

    fn handle_active_channels(
        &mut self,
        actives: HashSet<u64>,
        consumed: &mut Vec<EventChannel<Launcher>>,
    ) {
        for stream_id in actives {
            if let Some(stream) = self.streams.get_mut(&stream_id) {
                let mut scheduler = IoScheduler {
                    ctx: self.io_ctx.clone(),
                    stream: stream.master_stream.clone(),
                    channel: stream.channel.clone(),
                };
                stream.on_active(&mut scheduler);
                consumed.push(stream.channel.clone());
            }
        }
    }

    pub fn run(mut w: Worker, exit_flag: Arc<AtomicBool>) {
        let cloned_timer = w.mono_timer.clone();
        let join_handle = thread::spawn(|| {
            cloned_timer.run();
        });

        let mut action_channel = None;
        let mut consumed: Vec<EventChannel<Launcher>> = vec![];
        while !exit_flag.load(Ordering::Acquire) {
            let mut actives = w.selector.select(action_channel.take(), &consumed);
            consumed.clear();

            if actives.remove(&u64::MAX) {
                w.do_actions(&mut consumed);
                action_channel = Some(w.channel.clone());
            }

            w.handle_active_channels(actives, &mut consumed);
        }

        w.mono_timer.close();
        join_handle.join().unwrap_or_default();
    }
}
