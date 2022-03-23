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
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::channel::oneshot;
use stream_engine_proto::Record;

use super::partial_stream::{PartialStream, TxnContext};
use crate::{
    log::{LogEngine, LogFileManager},
    IoKindResult,
};

enum WaiterState {
    Writing(oneshot::Receiver<IoKindResult<u64>>),
    Received(Option<IoKindResult<u64>>),
}

pub(crate) trait WriterOwner {
    fn borrow_pipelined_writer_mut(
        &mut self,
    ) -> (&mut PartialStream<LogFileManager>, &mut PipelinedWriter);
}

pub(crate) struct WriteWaiter<T> {
    owner: Arc<Mutex<T>>,
    waiter_index: usize,
    state: WaiterState,
}

impl<T> WriteWaiter<T>
where
    T: WriterOwner,
{
    fn new(
        owner: Arc<Mutex<T>>,
        waiter_index: usize,
        receiver: oneshot::Receiver<IoKindResult<u64>>,
    ) -> Self {
        WriteWaiter {
            owner,
            waiter_index,
            state: WaiterState::Writing(receiver),
        }
    }

    fn received(owner: Arc<Mutex<T>>, waiter_index: usize) -> Self {
        WriteWaiter {
            owner,
            waiter_index,
            state: WaiterState::Received(None),
        }
    }

    fn failed(owner: Arc<Mutex<T>>, waiter_index: usize, err_kind: ErrorKind) -> Self {
        WriteWaiter {
            owner,
            waiter_index,
            state: WaiterState::Received(Some(Err(err_kind))),
        }
    }
}

impl<T> Future for WriteWaiter<T>
where
    T: WriterOwner,
{
    type Output = IoKindResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                WaiterState::Writing(ref mut receiver) => {
                    this.state = match futures::ready!(Pin::new(receiver).poll(cx)) {
                        Ok(result) => WaiterState::Received(Some(result)),
                        Err(_) => panic!("a waiter is canceled"),
                    };
                }
                WaiterState::Received(result) => {
                    let mut owner = this.owner.lock().unwrap();
                    let (stream, writer) = owner.borrow_pipelined_writer_mut();
                    if writer.waked_waiter_index + 1 == this.waiter_index {
                        return Poll::Ready(writer.apply_txn(
                            stream,
                            this.waiter_index,
                            result.take(),
                        ));
                    } else {
                        writer.wait_with_waker(this.waiter_index, cx.waker().clone());
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

pub(crate) struct PipelinedWriter {
    stream_id: u64,
    log_engine: LogEngine,

    last_error_kind: Option<ErrorKind>,
    next_waiter_index: usize,
    waked_waiter_index: usize,
    txn_table: HashMap<usize, TxnContext>,
    waiter_table: HashMap<usize, Waker>,
    reading_waiters: Vec<Waker>,
}

impl PipelinedWriter {
    pub fn new(stream_id: u64, log_engine: LogEngine) -> Self {
        PipelinedWriter {
            stream_id,
            log_engine,
            last_error_kind: None,
            next_waiter_index: 1,
            waked_waiter_index: 0,
            txn_table: HashMap::new(),
            waiter_table: HashMap::new(),
            reading_waiters: Vec::new(),
        }
    }

    pub fn submit<T>(
        &mut self,
        owner: Arc<Mutex<T>>,
        result: IoKindResult<Option<TxnContext>>,
    ) -> WriteWaiter<T>
    where
        T: WriterOwner,
    {
        match result {
            Ok(txn) => self.submit_txn(owner, txn),
            Err(err) => self.submit_barrier(owner, err),
        }
    }

    pub fn submit_txn<T>(&mut self, owner: Arc<Mutex<T>>, txn: Option<TxnContext>) -> WriteWaiter<T>
    where
        T: WriterOwner,
    {
        let waiter_index = self.next_waiter_index;
        self.next_waiter_index += 1;
        if let Some(txn) = txn {
            let record = convert_to_record(self.stream_id, &txn);
            let receiver = self.log_engine.add_record(record);
            self.txn_table.insert(waiter_index, txn);

            WriteWaiter::new(owner, waiter_index, receiver)
        } else {
            WriteWaiter::received(owner, waiter_index)
        }
    }

    pub fn submit_barrier<T>(&mut self, owner: Arc<Mutex<T>>, err_kind: ErrorKind) -> WriteWaiter<T>
    where
        T: WriterOwner,
    {
        let waiter_index = self.next_waiter_index;
        self.next_waiter_index += 1;
        WriteWaiter::failed(owner, waiter_index, err_kind)
    }

    #[inline(always)]
    pub fn register_reading_waiter(&mut self, waiter: Waker) {
        self.reading_waiters.push(waiter);
    }

    /// Apply txn and return the internal error.
    fn apply_txn(
        &mut self,
        stream: &mut PartialStream<LogFileManager>,
        waiter_index: usize,
        result: Option<IoKindResult<u64>>,
    ) -> IoKindResult<()> {
        debug_assert_eq!(self.waked_waiter_index + 1, waiter_index);
        let txn = self.txn_table.remove(&waiter_index);
        self.waked_waiter_index = waiter_index;

        match result {
            Some(Ok(log_number)) => {
                if let Some(txn) = txn {
                    stream.commit(log_number, txn);
                    self.wake_reading_waiters();
                }
            }
            Some(Err(err)) => {
                self.last_error_kind = Some(err);
                if let Some(txn) = txn {
                    stream.rollback(txn);
                }
            }
            None => {}
        }

        if let Some(a) = self.waiter_table.remove(&(waiter_index + 1)) {
            Waker::wake(a)
        }

        // FIXME(walter) how to determine the returned values of a failed pipeline?
        if let Some(err) = self.last_error_kind.take() {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn wait_with_waker(&mut self, waiter_index: usize, waker: Waker) {
        self.waiter_table.insert(waiter_index, waker);
    }

    #[inline(always)]
    fn wake_reading_waiters(&mut self) {
        std::mem::take(&mut self.reading_waiters)
            .into_iter()
            .for_each(Waker::wake);
    }
}

fn convert_to_record(stream_id: u64, txn: &TxnContext) -> Record {
    match txn {
        TxnContext::Sealed {
            segment_epoch,
            writer_epoch,
            ..
        } => Record {
            stream_id,
            epoch: *segment_epoch,
            writer_epoch: Some(*writer_epoch),
            first_index: None,
            acked_seq: None,
            entries: vec![],
        },
        TxnContext::Write {
            segment_epoch,
            first_index,
            acked_seq,
            entries,
            ..
        } => Record {
            stream_id,
            epoch: *segment_epoch,
            writer_epoch: None,
            first_index: Some(*first_index),
            acked_seq: Some((*acked_seq).into()),
            entries: entries.iter().cloned().map(Into::into).collect(),
        },
    }
}
