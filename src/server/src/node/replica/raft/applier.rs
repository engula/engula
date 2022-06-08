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

use std::collections::{HashMap, VecDeque};

use engula_api::server::v1::ReplicaDesc;
use futures::channel::oneshot;
use raft::{
    prelude::{ConfChangeV2, Entry, EntryType},
    RawNode, ReadState,
};

use super::{fsm::StateMachine, storage::Storage, ApplyEntry};
use crate::{serverpb::v1::EvalResult, Error, Result};

struct ProposalContext {
    index: u64,
    term: u64,
    sender: oneshot::Sender<Result<()>>,
}

/// Cache the descriptor of other replicas in the same group.
#[derive(Clone, Default)]
pub(super) struct ReplicaCache {
    replicas: HashMap<u64, ReplicaDesc>,
}

/// Applier records the context of requests.
pub struct Applier<M: StateMachine> {
    group_id: u64,

    proposal_queue: VecDeque<ProposalContext>,

    next_read_state_index: usize,
    read_requests: HashMap<Vec<u8>, Vec<oneshot::Sender<Result<()>>>>,

    /// ReadStates has been ready but wait the entries to apply.
    read_states: Vec<ReadState>,

    last_applied_index: u64,
    state_machine: M,
}

impl<M: StateMachine> Applier<M> {
    pub fn new(group_id: u64, state_machine: M) -> Self {
        Applier {
            group_id,
            proposal_queue: VecDeque::default(),
            next_read_state_index: 0,
            read_requests: HashMap::default(),
            read_states: Vec::default(),
            last_applied_index: state_machine.flushed_index(),
            state_machine,
        }
    }

    #[inline]
    pub fn delegate_proposal_context(
        &mut self,
        index: u64,
        term: u64,
        sender: oneshot::Sender<Result<()>>,
    ) {
        let ctx = ProposalContext {
            index,
            term,
            sender,
        };

        // ensure the proposals are monotonic.
        if let Some(last_ctx) = self.proposal_queue.back() {
            assert!(last_ctx.index < ctx.index);
        }
        self.proposal_queue.push_back(ctx);
    }

    pub fn delegate_read_requests(
        &mut self,
        requests: Vec<oneshot::Sender<Result<()>>>,
    ) -> Vec<u8> {
        let read_state_index = self.next_read_state_index;
        self.next_read_state_index += 1;

        let read_state_ctx: Vec<u8> = read_state_index.to_le_bytes().into();
        self.read_requests.insert(read_state_ctx.clone(), requests);
        read_state_ctx
    }

    /// Apply read states, cached if the target index haven't applied.
    pub fn apply_read_states(&mut self, mut read_states: Vec<ReadState>) {
        Self::response_read_states(
            &mut read_states,
            &mut self.read_requests,
            self.last_applied_index,
        );

        if !read_states.is_empty() {
            self.read_states.append(&mut read_states);
        }
    }

    /// Apply entries and invoke proposal & read response.
    pub(super) fn apply_entries(
        &mut self,
        raw_node: &mut RawNode<Storage>,
        replica_cache: &mut ReplicaCache,
        committed_entries: Vec<Entry>,
    ) -> u64 {
        for entry in committed_entries {
            self.last_applied_index = entry.index;
            if entry.data.is_empty() {
                self.state_machine
                    .apply(entry.index, entry.term, ApplyEntry::Empty)
                    .expect("apply empty entry");
                continue;
            }

            let index = entry.index;
            let term = entry.term;
            match entry.get_entry_type() {
                EntryType::EntryNormal => self.apply_normal_entry(entry),
                EntryType::EntryConfChange => panic!("ConfChangeV1 not supported"),
                EntryType::EntryConfChangeV2 => {
                    self.apply_conf_change(raw_node, replica_cache, entry)
                }
            }
            self.response_proposal(index, term);
        }

        // Since the `last_applied_index` updated, try advance cached read states.
        self.response_cached_read_states();
        self.last_applied_index
    }

    fn apply_conf_change(
        &mut self,
        raw_node: &mut RawNode<Storage>,
        replica_cache: &mut ReplicaCache,
        entry: Entry,
    ) {
        use prost::Message;

        assert!(matches!(
            entry.get_entry_type(),
            EntryType::EntryConfChangeV2
        ));

        let conf_change = ConfChangeV2::decode(&*entry.data).expect("decode ConfChangeV2");
        let change_replicas = super::decode_from_conf_change(&conf_change);
        self.state_machine
            .apply(
                entry.index,
                entry.term,
                ApplyEntry::ConfigChange { change_replicas },
            )
            .expect("apply config change");
        replica_cache.batch_insert(&self.state_machine.descriptor().replicas);
        raw_node.apply_conf_change(&conf_change).unwrap_or_default();
    }

    fn apply_normal_entry(&mut self, entry: Entry) {
        use prost::Message;

        assert!(matches!(entry.get_entry_type(), EntryType::EntryNormal));

        let eval_result = EvalResult::decode(&*entry.data).expect("Entry::data is EvalResult");
        self.state_machine
            .apply(
                entry.index,
                entry.term,
                ApplyEntry::Proposal { eval_result },
            )
            .expect("apply normal entry");
    }

    fn response_proposal(&mut self, index: u64, term: u64) {
        if self
            .proposal_queue
            .front()
            .map(|ctx| ctx.index == index)
            .unwrap_or_default()
        {
            let ctx = self.proposal_queue.pop_front().unwrap();
            if ctx.term == term {
                // TODO(walter) support user defined result.
                ctx.sender.send(Ok(())).unwrap_or_default();
            } else {
                ctx.sender
                    .send(Err(Error::NotLeader(self.group_id, None)))
                    .unwrap_or_default();
            }
        }
    }

    #[inline]
    fn response_cached_read_states(&mut self) {
        Self::response_read_states(
            &mut self.read_states,
            &mut self.read_requests,
            self.last_applied_index,
        );
    }

    fn response_read_states(
        read_states: &mut Vec<ReadState>,
        read_requests: &mut HashMap<Vec<u8>, Vec<oneshot::Sender<Result<()>>>>,
        last_applied_index: u64,
    ) {
        for rs in read_states.drain_filter(|rs| rs.index <= last_applied_index) {
            if let Some(requests) = read_requests.remove(&rs.request_ctx) {
                for request in requests {
                    request.send(Ok(())).unwrap_or_default();
                }
            }
        }
    }
}

impl ReplicaCache {
    #[inline]
    pub fn batch_insert(&mut self, replicas: &[ReplicaDesc]) {
        let cache = &mut self.replicas;
        for replica in replicas {
            cache.insert(replica.id, replica.clone());
        }
    }

    #[inline]
    pub fn insert(&mut self, replica: ReplicaDesc) {
        self.replicas.insert(replica.id, replica.clone());
    }

    pub fn get(&self, replica_id: u64) -> Option<ReplicaDesc> {
        self.replicas.get(&replica_id).cloned()
    }
}
