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
    collections::{BTreeMap, BTreeSet, HashMap},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::Stream;
use tonic::Status;

use super::{serverpb, Entry};
use crate::Sequence;

fn decode(seq: Sequence) -> (u32, u32) {
    ((seq >> 32) as u32, (seq & ((1 << 32) - 1)) as u32)
}

#[derive(Debug)]
struct Replica {
    bridge: Option<u32>,
    acked_index: Option<u32>,
    wakers: Vec<Waker>,
    entries: BTreeMap<u32, Entry>,
}

impl Replica {
    fn new() -> Self {
        Replica {
            bridge: None,
            acked_index: None,
            wakers: Vec::new(),
            entries: BTreeMap::new(),
        }
    }

    fn store(&mut self, first_index: u32, entries: Vec<Entry>) -> Result<(), Status> {
        for (off, entry) in entries.into_iter().enumerate() {
            let index = first_index + (off as u32);
            if self.bridge.map(|idx| index > idx).unwrap_or_default() {
                return Err(Status::invalid_argument(
                    "try to append a record after a bridge record",
                ));
            }
            if let Entry::Bridge { epoch: _ } = &entry {
                self.bridge = Some(index);
            }
            self.entries.insert(first_index + (off as u32), entry);
        }
        Ok(())
    }

    fn advance(&mut self, acked_index: u32) -> bool {
        if let Some(index) = &self.acked_index {
            if *index < acked_index {
                self.acked_index = Some(acked_index);
                true
            } else {
                false
            }
        } else {
            self.acked_index = Some(acked_index);
            true
        }
    }

    fn broadcast(&mut self) {
        // It's not efficient, but sufficient for verifying.
        std::mem::take(&mut self.wakers)
            .into_iter()
            .for_each(Waker::wake);
    }
}

type SharedReplica = Arc<Mutex<Replica>>;

#[derive(Debug)]
struct PartialStream {
    epochs: BTreeSet<u32>,
    replicas: HashMap<u32, SharedReplica>,
}

impl PartialStream {
    fn new() -> Self {
        PartialStream {
            epochs: BTreeSet::new(),
            replicas: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub(super) struct ReplicaReader {
    next_index: u32,
    limit: usize,
    finished: bool,

    replica: SharedReplica,
}

impl Stream for ReplicaReader {
    type Item = Result<serverpb::Entry, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }

        let mut replica = this.replica.lock().unwrap();
        if let Some(acked_index) = &replica.acked_index {
            if let Some((index, entry)) = replica.entries.range(this.next_index..).next() {
                // Continuous and acked.
                if acked_index < index || *index == this.next_index {
                    // End of segment.
                    if let Entry::Bridge { epoch: _ } = entry {
                        this.finished = true;
                    }
                    this.next_index += 1;
                    this.limit -= 1;
                    if this.limit == 0 {
                        this.finished = true;
                    }

                    return Poll::Ready(Some(Ok(entry.clone().into())));
                }
            }
        }

        replica.wakers.push(cx.waker().clone());

        Poll::Pending
    }
}

#[derive(Debug)]
#[allow(unused)]
pub(super) struct Store {
    streams: HashMap<u64, Box<PartialStream>>,
}

#[allow(dead_code)]
impl Store {
    pub fn new() -> Self {
        Store {
            streams: HashMap::new(),
        }
    }

    pub fn store(
        &mut self,
        stream_id: u64,
        seg_epoch: u32,
        acked_seq: u64,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(), Status> {
        let stream = self
            .streams
            .entry(stream_id)
            .or_insert_with(|| Box::new(PartialStream::new()));

        let replica = stream.replicas.entry(seg_epoch).or_insert_with(|| {
            stream.epochs.insert(seg_epoch);
            Arc::new(Mutex::new(Replica::new()))
        });

        let mut replica = replica.lock().unwrap();
        let mut updated = false;
        if !entries.is_empty() {
            updated = true;
            replica.store(first_index, entries)?;
        }

        let (acked_epoch, acked_index) = decode(acked_seq);
        if acked_epoch >= seg_epoch {
            updated = true;
            replica.advance(acked_index);
        }

        if updated {
            replica.broadcast();
        }

        Ok(())
    }

    pub fn read(
        &mut self,
        stream_id: u64,
        seg_epoch: u32,
        start_index: u32,
        limit: usize,
    ) -> Result<ReplicaReader, Status> {
        let stream = match self.streams.get_mut(&stream_id) {
            Some(s) => s,
            None => return Err(Status::not_found("no such stream")),
        };

        let replica = match stream.replicas.get_mut(&seg_epoch) {
            Some(r) => r,
            None => return Err(Status::not_found("no such segment replica exists")),
        };

        Ok(ReplicaReader {
            next_index: start_index,
            limit,
            finished: limit == 0,
            replica: replica.clone(),
        })
    }
}
