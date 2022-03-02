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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use log::{error, warn};
use stream_engine_proto::{
    Command, CommandType, ObserverState, ReadResponse, SegmentDesc, WriteRequest,
};
use tokio::runtime::Handle as RuntimeHandle;

use super::{
    stream::{EventChannel, Promote, Scheduler},
    worker::Launcher,
};
use crate::{
    core::{Learn, Learned, Message, MutKind, Mutate, Write},
    master::{ObserverMeta, Stream as MasterStream},
    store::Transport,
    Entry, Error, Result, Role, Sequence,
};

type TonicResult<T> = std::result::Result<T, tonic::Status>;

#[derive(Clone)]
pub struct IoContext {
    pub observer_id: String,
    pub runtime: RuntimeHandle,
    pub transport: Transport,
}

#[derive(Clone)]
pub(super) struct IoScheduler {
    pub ctx: Arc<IoContext>,
    pub stream: MasterStream,
    pub channel: EventChannel<Launcher>,
}

impl Scheduler for IoScheduler {
    fn send_heartbeat(
        &mut self,
        _role: Role,
        writer_epoch: u32,
        acked_seq: Sequence,
        observer_state: ObserverState,
    ) {
        let observer_meta = ObserverMeta {
            observer_id: self.ctx.observer_id.clone(),
            writer_epoch,
            state: observer_state,
            acked_seq,
        };
        let stream = self.stream.clone();
        let stream_id = stream.stream_id();
        let mut scheduler = self.clone();
        self.ctx.runtime.spawn(async move {
            match stream.heartbeat(observer_meta).await {
                Ok(commands) => {
                    scheduler.execute_master_command(commands).await;
                }
                Err(error) => {
                    warn!("stream {} send heartbeat: {}", stream_id, error);
                }
            }
        });
    }

    fn seal_segment(&mut self, segment_epoch: u32, writer_epoch: u32) {
        let stream = self.stream.clone();
        let channel = self.channel.clone();
        self.ctx.runtime.spawn(async move {
            match stream.seal_segment(segment_epoch).await {
                Ok(()) => {
                    channel.on_msg(Message::recovered(segment_epoch, writer_epoch));
                }
                Err(error) => {
                    error!(
                        "stream {} seal segment {}: {}",
                        stream.desc().id,
                        segment_epoch,
                        error
                    );
                    channel.on_msg(Message::master_timeout(segment_epoch, writer_epoch));
                }
            }
        });
    }

    fn handle_writes(&mut self, mutations: Vec<Mutate>) {
        for mutate in mutations {
            match mutate.kind {
                MutKind::Seal => {
                    self.flush_sealing(mutate.target, mutate.writer_epoch, mutate.seg_epoch)
                }
                MutKind::Write(detail) => {
                    self.flush_write(mutate.target, mutate.writer_epoch, mutate.seg_epoch, detail)
                }
            }
        }
    }

    fn handle_learns(&mut self, learns: Vec<Learn>) {
        for learn in learns {
            self.learn(learn);
        }
    }
}

impl IoScheduler {
    fn learn(&mut self, learn: Learn) {
        let stream_id = self.stream.stream_id();
        let transport = self.ctx.transport.clone();
        let channel = self.channel.clone();
        self.ctx.runtime.spawn(async move {
            let mut streaming = match transport
                .read(
                    learn.target.clone(),
                    stream_id,
                    learn.seg_epoch,
                    learn.start_index,
                    false,
                )
                .await
            {
                Ok(streaming) => streaming,
                Err(error) => {
                    warn!("stream {} learn entries: {}", stream_id, error);
                    channel.on_msg(Message::store_timeout(
                        learn.target,
                        learn.seg_epoch,
                        learn.writer_epoch,
                    ));
                    return;
                }
            };

            let mut streaming = TryBatchNext::new(&mut streaming);
            loop {
                match streaming.next().await {
                    Some(Ok(entries)) => {
                        channel.on_msg(Message::learned(
                            learn.target.clone(),
                            learn.seg_epoch,
                            learn.writer_epoch,
                            Learned { entries },
                        ));
                    }
                    Some(Err(status)) => {
                        warn!("stream {} learn entries: {}", stream_id, status);
                        break;
                    }
                    None => {
                        channel.on_msg(Message::learned(
                            learn.target.clone(),
                            learn.seg_epoch,
                            learn.writer_epoch,
                            Learned { entries: vec![] },
                        ));
                        break;
                    }
                }
            }
        });
    }

    fn flush_write(&mut self, target: String, writer_epoch: u32, segment_epoch: u32, write: Write) {
        let transport = self.ctx.transport.clone();
        let stream_id = self.stream.stream_id();
        let channel = self.channel.clone();
        self.ctx.runtime.spawn(async move {
            let write_req = WriteRequest {
                segment_epoch,
                acked_seq: write.acked_seq.into(),
                first_index: write.range.start,
                entries: write.entries.into_iter().map(Into::into).collect(),
            };
            let resp = transport
                .write(target.clone(), stream_id, writer_epoch, write_req)
                .await;
            match resp {
                Ok((matched_index, acked_index)) => {
                    channel.on_msg(Message::received(
                        target,
                        segment_epoch,
                        writer_epoch,
                        matched_index,
                        acked_index,
                    ));
                }
                Err(error) => {
                    error!(
                        "stream {} seal replica {}: {}",
                        stream_id, segment_epoch, error
                    );
                    channel.on_msg(Message::write_timeout(
                        target,
                        segment_epoch,
                        writer_epoch,
                        Some(write.range),
                        write.bytes,
                    ));
                }
            }
        });
    }

    fn flush_sealing(&mut self, target: String, writer_epoch: u32, segment_epoch: u32) {
        let transport = self.ctx.transport.clone();
        let stream_id = self.stream.stream_id();
        let channel = self.channel.clone();
        self.ctx.runtime.spawn(async move {
            let resp = transport
                .seal(target.clone(), stream_id, writer_epoch, segment_epoch)
                .await;
            match resp {
                Ok(acked_index) => {
                    channel.on_msg(Message::sealed(
                        target,
                        segment_epoch,
                        writer_epoch,
                        acked_index,
                    ));
                }
                Err(error) => {
                    error!(
                        "stream {} seal replica {}: {}",
                        stream_id, segment_epoch, error
                    );
                    channel.on_msg(Message::store_timeout(target, segment_epoch, writer_epoch));
                }
            }
        });
    }

    async fn execute_master_command(&mut self, commands: Vec<Command>) {
        for cmd in commands {
            match CommandType::from_i32(cmd.command_type) {
                Some(CommandType::Nop) | None => {}
                Some(CommandType::Promote) => {
                    self.promote(cmd).await;
                }
            }
        }
    }

    async fn get_segments(&mut self, pending_epochs: Vec<u32>) -> Result<Vec<SegmentDesc>> {
        self.stream
            .get_segments(pending_epochs)
            .await?
            .into_iter()
            .map(|d| d.ok_or_else(|| Error::NotFound("no such segment".to_owned())))
            .collect::<Result<Vec<_>>>()
    }

    async fn promote(&mut self, cmd: Command) {
        let stream_id = self.stream.stream_id();
        let mut pending_epochs = cmd.pending_epochs.clone();
        pending_epochs.push(cmd.epoch);
        let mut segments = match self.get_segments(pending_epochs).await {
            Ok(resp) => resp,
            Err(error) => {
                warn!("stream {} get segments: {}", stream_id, error);
                return;
            }
        };
        debug_assert_eq!(segments.len(), cmd.pending_epochs.len() + 1);
        let new_seg = segments.pop().unwrap();
        let promote = Box::new(Promote {
            role: cmd.role.into(),
            epoch: cmd.epoch,
            leader: cmd.leader,
            copy_set: new_seg.copy_set,
            broken_segments: segments,
        });
        self.channel.on_promote(promote);
    }
}

struct TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<ReadResponse>>,
{
    stream: &'a mut S,
    terminated: Option<TonicResult<()>>,
    entries: Vec<(u32, Entry)>,
}

impl<'a, S> TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<ReadResponse>>,
{
    fn new(stream: &'a mut S) -> Self {
        TryBatchNext {
            stream,
            terminated: None,
            entries: Vec::default(),
        }
    }
}

impl<'a, S> Stream for TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<ReadResponse>> + Unpin,
{
    type Item = TonicResult<Vec<(u32, Entry)>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while this.terminated.is_none() {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(resp)) => match resp {
                    Ok(resp) => this.entries.push((resp.index, resp.entry.unwrap().into())),
                    Err(status) => {
                        this.terminated = Some(Err(status));
                    }
                },
                Poll::Ready(None) => {
                    this.terminated = Some(Ok(()));
                }
                Poll::Pending => {
                    if this.entries.is_empty() {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Some(Ok(std::mem::take(&mut this.entries))));
                    }
                }
            }
        }

        if !this.entries.is_empty() {
            return Poll::Ready(Some(Ok(std::mem::take(&mut this.entries))));
        }

        match std::mem::replace(&mut this.terminated, Some(Ok(()))) {
            Some(Ok(())) => Poll::Ready(None),
            Some(Err(status)) => Poll::Ready(Some(Err(status))),
            None => unreachable!(),
        }
    }
}
