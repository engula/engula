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

use engula_api::server::v1::RaftRole;
use futures::channel::oneshot;
use raft::{prelude::*, ConfChangeI, StateRole, Storage as RaftStorage};
use raft_engine::LogBatch;
use tracing::{info, trace};

use super::{
    applier::{Applier, ReplicaCache},
    fsm::StateMachine,
    snap::apply::apply_snapshot,
    storage::Storage,
    RaftManager, SnapManager,
};
use crate::{Error, Result};

/// WriteTask records the metadata and entries to persist to disk.
#[derive(Default)]
pub struct WriteTask {
    pub hard_state: Option<HardState>,
    pub entries: Vec<Entry>,

    /// Snapshot specifies the snapshot to be saved to stable storage.
    pub snapshot: Option<Snapshot>,
    pub must_sync: bool,
    post_ready: PostReady,
}

#[derive(Default)]
pub struct PostReady {
    /// The number of ready. See `raft::raw_node::Ready` for details.
    number: u64,

    persisted_messages: Vec<Message>,
    snap_index: Option<u64>,
}

pub(super) trait AdvanceTemplate {
    fn send_messages(&mut self, msgs: Vec<Message>);

    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole);

    fn mut_replica_cache(&mut self) -> &mut ReplicaCache;

    fn apply_snapshot<M: StateMachine>(&mut self, applier: &mut Applier<M>, snapshot: &Snapshot);
}

pub struct RaftNode<M: StateMachine> {
    group_id: u64,

    lease_read_requests: Vec<oneshot::Sender<Result<()>>>,
    read_index_requests: Vec<oneshot::Sender<Result<()>>>,
    read_states: Vec<ReadState>,

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
        let mut applier = Applier::new(group_id, state_machine);
        try_apply_fresh_snapshot(replica_id, &mgr.snap_mgr, &mut applier).await;

        let cfg = &mgr.cfg;
        let applied = applier.flushed_index();
        let conf_state =
            super::conf_state_from_group_descriptor(&applier.mut_state_machine().descriptor());
        let mut storage = Storage::open(
            cfg,
            replica_id,
            applied,
            conf_state,
            mgr.engine.clone(),
            mgr.snap_mgr.clone(),
        )
        .await?;
        try_reset_storage_state(replica_id, &mgr.snap_mgr, &mgr.engine, &mut storage).await?;

        let config = Config {
            id: replica_id,
            election_tick: cfg.election_tick,
            heartbeat_tick: 1,
            applied,
            pre_vote: true,
            batch_append: true,
            check_quorum: true,
            max_size_per_msg: cfg.max_size_per_msg,
            max_inflight_msgs: cfg.max_inflight_msgs,
            max_committed_size_per_ready: cfg.max_io_batch_size,
            read_only_option: ReadOnlyOption::Safe,
            ..Default::default()
        };
        Ok(RaftNode {
            group_id,
            lease_read_requests: Vec::default(),
            read_index_requests: Vec::default(),
            read_states: Vec::default(),
            raw_node: RawNode::with_default_logger(&config, storage)?,
            applier,
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
                .send(Err(Error::NotLeader(
                    self.group_id,
                    self.raw_node.raft.term,
                    None,
                )))
                .unwrap_or_default();
            return;
        }

        if let Err(err) = self.raw_node.propose(context, data) {
            if matches!(err, raft::Error::ProposalDropped) {
                sender
                    .send(Err(Error::ServiceIsBusy("proposal dropped")))
                    .unwrap_or_default();
            } else {
                sender.send(Err(err.into())).unwrap_or_default();
            }
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
                .send(Err(Error::NotLeader(
                    self.group_id,
                    self.raw_node.raft.term,
                    None,
                )))
                .unwrap_or_default();
            return;
        }

        if let Err(err) = self.raw_node.propose_conf_change(context, cc) {
            if matches!(err, raft::Error::ProposalDropped) {
                sender
                    .send(Err(Error::ServiceIsBusy("proposal dropped")))
                    .unwrap_or_default();
            } else {
                sender.send(Err(err.into())).unwrap_or_default();
            }
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
        if msg.get_msg_type() == MessageType::MsgSnapStatus {
            self.raw_node.report_snapshot(
                msg.from,
                if msg.reject {
                    SnapshotStatus::Failure
                } else {
                    SnapshotStatus::Finish
                },
            );
            Ok(())
        } else {
            match self.raw_node.step(msg) {
                Ok(()) | Err(raft::Error::StepPeerNotFound) => Ok(()),
                Err(e) => Err(e),
            }
        }
    }

    fn advance_read_requests(&mut self) {
        if !self.lease_read_requests.is_empty() {
            let requests = std::mem::take(&mut self.lease_read_requests);
            if self.raw_node.raft.state != StateRole::Leader {
                for req in requests {
                    req.send(Err(Error::NotLeader(
                        self.group_id,
                        self.raw_node.raft.term,
                        None,
                    )))
                    .unwrap_or_default();
                }
            } else {
                debug_assert!(self.raw_node.raft.commit_to_current_term());
                let read_state_ctx = self.applier.delegate_read_requests(requests);
                self.read_states.push(ReadState {
                    index: self.committed_index(),
                    request_ctx: read_state_ctx,
                });
            }
        }

        if !self.read_index_requests.is_empty() {
            let requests = std::mem::take(&mut self.read_index_requests);
            let read_state_ctx = self.applier.delegate_read_requests(requests);
            self.raw_node.read_index(read_state_ctx);
        }
    }

    #[inline]
    pub fn has_ready(&mut self) -> bool {
        self.raw_node.has_ready()
    }

    /// Advance raft node, persist, apply entries and send messages.
    pub(super) fn advance(&mut self, template: &mut impl AdvanceTemplate) -> Option<WriteTask> {
        self.advance_read_requests();
        if !self.raw_node.has_ready() {
            if !self.read_states.is_empty() {
                self.applier
                    .apply_read_states(std::mem::take(&mut self.read_states));
            }
            return None;
        }

        let mut ready = self.raw_node.ready();
        if let Some(ss) = ready.ss() {
            let state = match ss.raft_state {
                StateRole::Candidate => RaftRole::Candidate,
                StateRole::Follower => RaftRole::Follower,
                StateRole::PreCandidate => RaftRole::PreCandidate,
                StateRole::Leader => RaftRole::Leader,
            };
            template.on_state_updated(
                ss.leader_id,
                self.raw_node.raft.vote,
                self.raw_node.raft.term,
                state,
            );
        }

        if !ready.messages().is_empty() {
            template.send_messages(ready.take_messages());
        }

        self.handle_apply(template, &mut ready);

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

    pub(super) fn post_advance(
        &mut self,
        post_ready: PostReady,
        sender: &mut impl AdvanceTemplate,
    ) {
        if !post_ready.persisted_messages.is_empty() {
            sender.send_messages(post_ready.persisted_messages);
        }

        self.raw_node.on_persist_ready(post_ready.number);
        if let Some(snap_index) = post_ready.snap_index {
            self.raw_node.advance_apply_to(snap_index);
        }
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut Storage {
        self.raw_node.raft.mut_store()
    }

    #[inline]
    pub fn mut_state_machine(&mut self) -> &mut M {
        self.applier.mut_state_machine()
    }

    #[inline]
    pub fn raft(&self) -> &Raft<Storage> {
        &self.raw_node.raft
    }

    #[inline]
    pub fn raft_status(&self) -> raft::Status {
        self.raw_node.status()
    }

    #[inline]
    pub fn committed_index(&self) -> u64 {
        self.raw_node.raft.raft_log.committed
    }

    fn handle_apply(&mut self, template: &mut impl AdvanceTemplate, ready: &mut Ready) {
        if !self.read_states.is_empty() {
            self.applier
                .apply_read_states(std::mem::take(&mut self.read_states));
        }

        if !ready.read_states().is_empty() {
            self.applier.apply_read_states(ready.take_read_states());
        }

        if !ready.committed_entries().is_empty() {
            trace!(
                "apply committed entries {}",
                ready.committed_entries().len()
            );
            let replica_cache = template.mut_replica_cache();
            let applied = self.applier.apply_entries(
                &mut self.raw_node,
                replica_cache,
                ready.take_committed_entries(),
            );
            self.raw_node.advance_apply_to(applied);

            let last_applied_index = self.applier.applied_index();
            self.raw_node.mut_store().post_apply(last_applied_index);
        }

        if !ready.snapshot().is_empty() {
            template.apply_snapshot(&mut self.applier, ready.snapshot());
        }
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
            must_sync: ready.must_sync(),
        };

        if !ready.snapshot().is_empty() {
            write_task.snapshot = Some(ready.snapshot().clone());
        }

        Some(write_task)
    }
}

impl PostReady {
    pub fn new(ready: &mut Ready) -> Self {
        let snap_index = if ready.snapshot().is_empty() {
            None
        } else {
            Some(ready.snapshot().get_metadata().get_index())
        };
        PostReady {
            number: ready.number(),
            persisted_messages: ready.take_persisted_messages(),
            snap_index,
        }
    }
}

impl WriteTask {
    #[cfg(test)]
    pub fn with_entries(entries: Vec<Entry>) -> Self {
        WriteTask {
            entries,
            ..Default::default()
        }
    }

    pub fn post_ready(self) -> PostReady {
        self.post_ready
    }
}

async fn try_apply_fresh_snapshot<M>(
    replica_id: u64,
    snap_mgr: &SnapManager,
    applier: &mut Applier<M>,
) where
    M: StateMachine,
{
    if let Some(info) = snap_mgr.latest_snap(replica_id) {
        let apply_state = info.meta.apply_state.as_ref().unwrap();
        if applier.flushed_index() < apply_state.index {
            info!(
                "replica {replica_id} apply fresh snapshot index {} term {}, local applied index {}",
                apply_state.index, apply_state.term,
                applier.flushed_index()
            );
            apply_snapshot(replica_id, snap_mgr, applier, &info.to_raft_snapshot());
        }
    }
}

async fn try_reset_storage_state(
    replica_id: u64,
    snap_mgr: &SnapManager,
    engine: &raft_engine::Engine,
    storage: &mut Storage,
) -> Result<()> {
    if let Some(info) = snap_mgr.latest_snap(replica_id) {
        // Two cases:
        // 1. out of range
        // 2. term mismatch
        //
        // Don't need to check `storage.truncated_index() == apply_state.index`.
        let apply_state = info.meta.apply_state.as_ref().unwrap();
        if storage.truncated_index() < apply_state.index
            && (!storage.range().contains(&apply_state.index)
                || storage.term(apply_state.index).unwrap() < apply_state.term)
        {
            info!(
                "replica {replica_id} reset storage with snapshot index {}, term {}",
                apply_state.index, apply_state.term
            );

            let task = WriteTask {
                snapshot: Some(info.to_raft_snapshot()),
                ..Default::default()
            };
            let mut lb = LogBatch::default();
            storage.write(&mut lb, &task)?;
            engine.write(&mut lb, true)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use engula_api::server::v1::{GroupDesc, NodeDesc, ReplicaDesc, ReplicaRole};
    use raft_engine::*;

    use super::*;
    use crate::{
        node::RaftRouteTable,
        raftgroup::{write_initial_state, AddressResolver, TransportManager},
        runtime::ExecutorOwner,
        serverpb::v1::{ApplyState, EvalResult, SnapshotMeta},
        RaftConfig,
    };

    struct SimpleStateMachine {
        current_snapshot: Option<PathBuf>,
        flushed_index: u64,
    }
    impl StateMachine for SimpleStateMachine {
        fn start_plug(&mut self) -> crate::Result<()> {
            Ok(())
        }

        #[allow(unused)]
        fn apply(
            &mut self,
            index: u64,
            term: u64,
            entry: crate::raftgroup::ApplyEntry,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn finish_plug(&mut self) -> crate::Result<()> {
            Ok(())
        }

        fn apply_snapshot(&mut self, snap_dir: &std::path::Path) -> crate::Result<()> {
            self.current_snapshot = Some(snap_dir.to_owned());
            Ok(())
        }

        fn snapshot_builder(&self) -> Box<dyn crate::raftgroup::SnapshotBuilder> {
            todo!()
        }

        fn descriptor(&self) -> engula_api::server::v1::GroupDesc {
            todo!()
        }

        fn flushed_index(&self) -> u64 {
            self.flushed_index
        }
    }

    async fn try_recover_snapshot<M>(
        replica_id: u64,
        snap_mgr: &SnapManager,
        engine: &raft_engine::Engine,
        storage: &mut Storage,
        applier: &mut Applier<M>,
    ) -> crate::Result<()>
    where
        M: StateMachine,
    {
        try_apply_fresh_snapshot(replica_id, snap_mgr, applier).await;
        try_reset_storage_state(replica_id, snap_mgr, engine, storage).await
    }

    fn create_snapshot(snap_mgr: &SnapManager, replica_id: u64, index: u64, term: u64) -> PathBuf {
        use prost::Message;

        let snap_dir = snap_mgr.create(replica_id);
        std::fs::create_dir_all(&snap_dir).unwrap();
        let data = snap_dir.join("DATA");
        let meta = SnapshotMeta {
            apply_state: Some(ApplyState { index, term }),
            group_desc: Some(GroupDesc::default()),
            ..Default::default()
        };
        std::fs::write(&data, meta.encode_to_vec()).unwrap();
        snap_mgr.install(replica_id, &snap_dir, &meta);
        snap_dir
    }

    #[test]
    fn recover_snapshot() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async {
            use raft_engine::Config;

            let dir = tempdir::TempDir::new("raftgroup-recover-snapshot").unwrap();
            let cfg = Config {
                dir: dir.path().join("db").to_str().unwrap().to_owned(),
                ..Default::default()
            };
            let engine = Arc::new(Engine::open(cfg).unwrap());

            write_initial_state(&RaftConfig::default(), engine.as_ref(), 1, vec![], vec![])
                .await
                .unwrap();

            let snap_dir = dir.path().join("snap");
            let snap_mgr = SnapManager::new(snap_dir.clone());
            let mut storage = Storage::open(
                &RaftConfig::default(),
                1,
                0,
                ConfState::default(),
                engine.clone(),
                snap_mgr.clone(),
            )
            .await
            .unwrap();
            let state_machine = SimpleStateMachine {
                flushed_index: 1,
                current_snapshot: None,
            };
            let mut applier = Applier::new(1, state_machine);

            // 1. recover nothing
            try_recover_snapshot(1, &snap_mgr, &engine, &mut storage, &mut applier)
                .await
                .unwrap();
            assert!(applier.mut_state_machine().current_snapshot.is_none());

            // 2. recovery snapshot
            let snap = create_snapshot(&snap_mgr, 1, 123, 1);

            try_recover_snapshot(1, &snap_mgr, &engine, &mut storage, &mut applier)
                .await
                .unwrap();
            assert!(
                matches!(applier.mut_state_machine().current_snapshot.clone(),
                Some(v) if v == snap.join("DATA")),
                "expect {}, got {:?}",
                snap.display(),
                applier
                    .mut_state_machine()
                    .current_snapshot
                    .as_ref()
                    .map(|v| v.display())
            );
        });
    }

    #[test]
    fn recover_with_staled_snapshot() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async {
            use raft_engine::Config;

            let dir = tempdir::TempDir::new("raftgroup-recover-staled-snapshot").unwrap();
            let cfg = Config {
                dir: dir.path().join("db").to_str().unwrap().to_owned(),
                ..Default::default()
            };
            let engine = Arc::new(Engine::open(cfg).unwrap());

            write_initial_state(&RaftConfig::default(), engine.as_ref(), 1, vec![], vec![])
                .await
                .unwrap();

            let snap_dir = dir.path().join("snap");
            let snap_mgr = SnapManager::new(snap_dir.clone());
            let mut storage = Storage::open(
                &RaftConfig::default(),
                1,
                0,
                ConfState::default(),
                engine.clone(),
                snap_mgr.clone(),
            )
            .await
            .unwrap();
            let state_machine = SimpleStateMachine {
                flushed_index: 123,
                current_snapshot: None,
            };
            let mut applier = Applier::new(1, state_machine);

            insert_entries(
                engine.clone(),
                &mut storage,
                (1..100).into_iter().map(|i| (i, 1)).collect(),
            )
            .await;
            storage.compact_to(51);

            // create staled snapshot.
            create_snapshot(&snap_mgr, 1, 10, 1);

            try_recover_snapshot(1, &snap_mgr, &engine, &mut storage, &mut applier)
                .await
                .unwrap();
            assert!(applier.mut_state_machine().current_snapshot.is_none());
            assert_eq!(storage.truncated_index(), 50);
        });
    }

    async fn insert_entries(engine: Arc<Engine>, storage: &mut Storage, entries: Vec<(u64, u64)>) {
        let entries: Vec<Entry> = entries
            .into_iter()
            .map(|(idx, term)| {
                let mut e = Entry::default();
                e.set_index(idx);
                e.set_term(term);
                e
            })
            .collect();

        let mut lb = LogBatch::default();
        let wt = WriteTask::with_entries(entries);
        storage.write(&mut lb, &wt).unwrap();
        engine.write(&mut lb, false).unwrap();
    }

    // Snapshot has been applied, but the storage haven't reset.
    #[test]
    fn partial_recover_snapshot() {
        let owner = ExecutorOwner::new(1);
        owner.executor().block_on(async {
            use raft_engine::Config;

            let dir = tempdir::TempDir::new("raftgroup-partial-recover-snapshot").unwrap();
            let cfg = Config {
                dir: dir.path().join("db").to_str().unwrap().to_owned(),
                ..Default::default()
            };
            let engine = Arc::new(Engine::open(cfg).unwrap());

            write_initial_state(&RaftConfig::default(), engine.as_ref(), 1, vec![], vec![])
                .await
                .unwrap();

            let snap_dir = dir.path().join("snap");
            let snap_mgr = SnapManager::new(snap_dir.clone());
            let mut storage = Storage::open(
                &RaftConfig::default(),
                1,
                0,
                ConfState::default(),
                engine.clone(),
                snap_mgr.clone(),
            )
            .await
            .unwrap();
            let state_machine = SimpleStateMachine {
                flushed_index: 123,
                current_snapshot: None,
            };
            let mut applier = Applier::new(1, state_machine);

            create_snapshot(&snap_mgr, 1, 123, 123);

            // case 1: snapshot exceeds log storage range.
            try_recover_snapshot(1, &snap_mgr, &engine, &mut storage, &mut applier)
                .await
                .unwrap();
            assert!(applier.mut_state_machine().current_snapshot.is_none());
            assert_eq!(storage.truncated_index(), 123);

            // case 2: snapshot term is bigger.
            insert_entries(
                engine.clone(),
                &mut storage,
                vec![(124, 123), (125, 123), (126, 123)],
            )
            .await;
            applier.mut_state_machine().flushed_index = 125;
            create_snapshot(&snap_mgr, 1, 125, 124);

            try_recover_snapshot(1, &snap_mgr, &engine, &mut storage, &mut applier)
                .await
                .unwrap();
            assert!(applier.mut_state_machine().current_snapshot.is_none());
            assert_eq!(storage.truncated_index(), 125);
        });
    }

    /// After restoring the state machine data from the snapshot, the index of the submitted apply
    /// task must be monotonically increasing (the first one should be equal to snapshot.index + 1).
    #[test]
    fn recover_from_snapshot_with_consistent_applied_index() {
        struct MockedAddressResolver {}

        #[crate::async_trait]
        impl AddressResolver for MockedAddressResolver {
            async fn resolve(&self, _: u64) -> crate::Result<NodeDesc> {
                todo!()
            }
        }

        struct CheckIndexStateMachine {
            flushed_index: u64,
        }

        impl StateMachine for CheckIndexStateMachine {
            fn start_plug(&mut self) -> crate::Result<()> {
                Ok(())
            }

            #[allow(unused)]
            fn apply(
                &mut self,
                index: u64,
                term: u64,
                entry: crate::raftgroup::ApplyEntry,
            ) -> crate::Result<()> {
                assert_eq!(self.flushed_index + 1, index);
                self.flushed_index = index;
                Ok(())
            }

            fn finish_plug(&mut self) -> crate::Result<()> {
                Ok(())
            }

            fn apply_snapshot(&mut self, data: &std::path::Path) -> crate::Result<()> {
                use prost::Message;

                let content = std::fs::read(data).unwrap();
                let meta = SnapshotMeta::decode(&*content).unwrap();
                self.flushed_index = meta.apply_state.unwrap().index;
                Ok(())
            }

            fn snapshot_builder(&self) -> Box<dyn crate::raftgroup::SnapshotBuilder> {
                todo!()
            }

            fn descriptor(&self) -> engula_api::server::v1::GroupDesc {
                // Only one member, so the node will become leader immediately.
                GroupDesc {
                    id: 1,
                    epoch: 1,
                    shards: vec![],
                    replicas: vec![ReplicaDesc {
                        id: 1,
                        role: ReplicaRole::Voter as i32,
                        ..Default::default()
                    }],
                }
            }

            fn flushed_index(&self) -> u64 {
                self.flushed_index
            }
        }

        let owner = ExecutorOwner::new(1);
        let executor = owner.executor();
        owner.executor().block_on(async {
            use raft_engine::Config;

            let dir = tempdir::TempDir::new("raftgroup-partial-recover-snapshot").unwrap();
            let cfg = Config {
                dir: dir.path().join("db").to_str().unwrap().to_owned(),
                ..Default::default()
            };
            let engine = Arc::new(Engine::open(cfg).unwrap());
            let snap_dir = dir.path().join("snap");
            let snap_mgr = SnapManager::new(snap_dir.clone());
            let resolver = Arc::new(MockedAddressResolver {});
            let transport_mgr =
                TransportManager::build(executor.clone(), resolver, RaftRouteTable::new());
            let raft_mgr = RaftManager {
                cfg: RaftConfig::default(),
                executor,
                engine: engine.clone(),
                transport_mgr,
                snap_mgr: snap_mgr.clone(),
            };

            // 1. initial storage with log entries in [0, 100), all entries are committed.
            write_initial_state(
                &RaftConfig::default(),
                engine.as_ref(),
                1,
                vec![ReplicaDesc {
                    id: 1,
                    ..Default::default()
                }],
                (0..100)
                    .into_iter()
                    .map(|_| EvalResult::default())
                    .collect(),
            )
            .await
            .unwrap();

            // 2. create snapshot with index 50 term 1.
            // See storage::write_initial_state for details about term 1.
            create_snapshot(&snap_mgr, 1, 50, 1);

            // 3. recover node from snapshot. and apply all entries.
            let state_machine = CheckIndexStateMachine { flushed_index: 0 };
            let mut node = RaftNode::new(1, 1, &raft_mgr, state_machine).await.unwrap();
            assert_eq!(node.mut_state_machine().flushed_index(), 50);
            node.raw_node.campaign().unwrap();

            // 4. apply left entries.
            struct AdvanceTemplateImpl {
                snap_mgr: SnapManager,
                replica_cache: ReplicaCache,
            }

            impl AdvanceTemplate for AdvanceTemplateImpl {
                fn send_messages(&mut self, _: Vec<Message>) {}

                fn on_state_updated(&mut self, _: u64, _: u64, _: u64, _: RaftRole) {}

                fn mut_replica_cache(&mut self) -> &mut ReplicaCache {
                    &mut self.replica_cache
                }

                fn apply_snapshot<M: StateMachine>(
                    &mut self,
                    applier: &mut Applier<M>,
                    snapshot: &Snapshot,
                ) {
                    use crate::raftgroup::snap::apply::apply_snapshot;
                    apply_snapshot(1, &self.snap_mgr, applier, snapshot);
                }
            }

            let mut template = AdvanceTemplateImpl {
                snap_mgr: snap_mgr.clone(),
                replica_cache: ReplicaCache::default(),
            };

            while let Some(task) = node.advance(&mut template) {
                let mut batch = LogBatch::default();
                node.mut_store()
                    .write(&mut batch, &task)
                    .expect("write log batch");
                engine.write(&mut batch, false).unwrap();
                node.post_advance(task.post_ready(), &mut template)
            }
            assert!(node.mut_state_machine().flushed_index() >= 100);
        });
    }
}
