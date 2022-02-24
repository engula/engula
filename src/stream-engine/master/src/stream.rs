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

use std::{collections::HashMap, ops::DerefMut, sync::Arc, time::Instant};

use log::{debug, info};
use stream_engine_proto::{
    Command, CommandType, ObserverState, Role, SegmentDesc, SegmentState, StreamDesc,
};
use tokio::sync::Mutex;

use crate::{master::Config, Error, Result, Sequence};

const INITIAL_EPOCH: u32 = 0;

#[derive(Debug)]
struct PolicyApplicant<'a> {
    role: Role,
    epoch: u32,
    observer_id: String,
    stores: &'a [String],
}

pub(crate) const DEFAULT_NUM_THRESHOLD: u32 = 1024;

#[derive(Debug, Clone, Copy)]
pub struct ThresholdSwitching {}

impl ThresholdSwitching {
    fn new() -> Self {
        // TODO(w41ter) support size option.
        ThresholdSwitching {}
    }

    fn apply(&self, applicant: &PolicyApplicant, stream_info: &mut StreamInner) -> Option<Command> {
        if let Role::Leader = applicant.role {
            if let Some(segment_info) = stream_info.segments.get(&stream_info.epoch) {
                if segment_info.acked_index > DEFAULT_NUM_THRESHOLD {
                    return Some(stream_info.elect_leader(applicant));
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy)]
enum SwitchPolicy {
    Threshold(ThresholdSwitching),
}

impl SwitchPolicy {
    fn apply(&self, applicant: &PolicyApplicant, stream_info: &mut StreamInner) -> Option<Command> {
        match self {
            SwitchPolicy::Threshold(policy) => policy.apply(applicant, stream_info),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct ObserverMeta {
    pub observer_id: String,

    /// Which stream is observing?
    pub stream_name: String,

    /// The value of epoch in observer's memory.
    pub epoch: u32,

    pub state: ObserverState,

    /// The acked sequence of entries it has already known. It might less than
    /// (epoch << 32).
    pub acked_seq: Sequence,
}

#[derive(Debug)]
struct ObserverInfo {
    meta: ObserverMeta,
    #[allow(dead_code)]
    role: Role,
    last_heartbeat: Instant,
}

#[derive(Debug)]
pub(crate) struct SegmentInfo {
    epoch: u32,
    acked_index: u32,
    state: SegmentState,
    copy_set: Vec<String>,
}

impl SegmentInfo {
    fn new(epoch: u32, copy_set: Vec<String>) -> Self {
        SegmentInfo {
            epoch,
            acked_index: 0,
            state: SegmentState::Appending,
            copy_set,
        }
    }

    fn segment_desc(&self, stream_id: u64) -> SegmentDesc {
        SegmentDesc {
            stream_id,
            epoch: self.epoch,
            state: self.state as i32,
            copy_set: self.copy_set.clone(),
        }
    }
}

#[derive(Clone)]
pub struct StreamInfo {
    pub parent_id: u64,
    pub stream_id: u64,
    pub stream_name: String,

    inner: Arc<Mutex<StreamInner>>,
}

struct StreamInner {
    /// The latest allocated epoch of this stream.
    epoch: u32,

    switch_policy: Option<SwitchPolicy>,

    segments: HashMap<u32, SegmentInfo>,
    leader: Option<String>,
    observers: HashMap<String, ObserverInfo>,
}

impl StreamInfo {
    pub fn new(parent_id: u64, stream_id: u64, stream_name: String) -> Self {
        // TODO(w41ter) support configuring switch policy.
        StreamInfo {
            parent_id,
            stream_id,
            stream_name,

            inner: Arc::new(Mutex::new(StreamInner {
                epoch: INITIAL_EPOCH,
                switch_policy: Some(SwitchPolicy::Threshold(ThresholdSwitching::new())),
                segments: HashMap::new(),
                leader: None,
                observers: HashMap::new(),
            })),
        }
    }

    pub fn stream_desc(&self) -> StreamDesc {
        StreamDesc {
            id: self.stream_id,
            name: self.stream_name.clone(),
            parent_id: self.parent_id,
        }
    }

    pub async fn segment(&self, segment_epoch: u32) -> Option<SegmentDesc> {
        let inner = self.inner.lock().await;
        inner
            .segments
            .get(&segment_epoch)
            .map(|s| s.segment_desc(self.stream_id))
    }

    pub async fn seal(&self, segment_epoch: u32) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if let Some(segment) = inner.segments.get_mut(&segment_epoch) {
            if segment.state != SegmentState::Sealed {
                segment.state = SegmentState::Sealed;
            }
            Ok(())
        } else {
            Err(Error::NotFound("no such segment".into()))
        }
    }

    pub async fn heartbeat(
        &self,
        config: &Config,
        stores: &[String],
        observer_meta: ObserverMeta,
        role: Role,
    ) -> Result<Vec<Command>> {
        let writer_epoch = observer_meta.epoch;
        let observer_id = observer_meta.observer_id.clone();
        let observer_info = ObserverInfo {
            meta: observer_meta,
            role,
            last_heartbeat: Instant::now(),
        };

        let mut stream = self.inner.lock().await;
        let stream = stream.deref_mut();
        if stream.epoch < writer_epoch && stream.epoch != INITIAL_EPOCH {
            return Err(Error::InvalidArgument("too large epoch".into()));
        }

        stream.observe(&observer_id, observer_info);

        let applicant = PolicyApplicant {
            epoch: writer_epoch,
            role,
            observer_id,
            stores,
        };
        Ok(apply_strategies(self.stream_id, config, &applicant, stream))
    }
}

impl StreamInner {
    pub fn leader_desc(&self) -> &str {
        if let Some(leader) = self.leader.as_ref() {
            leader
        } else {
            ""
        }
    }

    pub fn observe(&mut self, observer_id: &str, observer_info: ObserverInfo) -> bool {
        if let Some(prev_info) = self.observers.get(observer_id) {
            if prev_info.last_heartbeat > observer_info.last_heartbeat {
                return false;
            }
        }

        let acked_seq = observer_info.meta.acked_seq;
        let acked_epoch = acked_seq.epoch;
        let acked_index = acked_seq.index;

        debug!(
            "{:?} acked epoch: {}, acked index {}",
            observer_info, acked_epoch, acked_index
        );

        self.observers.insert(observer_id.into(), observer_info);
        if let Some(segment_info) = self.segments.get_mut(&acked_epoch) {
            // TODO(w41ter) update all previous epochs.
            segment_info.epoch = acked_epoch;
            segment_info.acked_index = acked_index;
        }

        true
    }

    pub fn elect_leader(&mut self, applicant: &PolicyApplicant) -> Command {
        self.epoch += 1;
        self.leader = Some(applicant.observer_id.clone());
        self.segments.insert(
            self.epoch,
            SegmentInfo::new(self.epoch, applicant.stores.into()),
        );
        self.gen_promote_cmd(&applicant.observer_id)
    }

    pub fn gen_promote_cmd(&self, observer_id: &str) -> Command {
        // TODO(w41ter) set pending epochs.
        if let Some(leader) = &self.leader {
            if leader == observer_id {
                return Command {
                    command_type: CommandType::Promote as i32,
                    epoch: self.epoch,
                    role: Role::Leader as i32,
                    leader: observer_id.to_string(),
                    pending_epochs: vec![],
                };
            }
        }
        Command {
            command_type: CommandType::Promote as i32,
            role: Role::Follower as i32,
            epoch: self.epoch,
            leader: self.leader.as_ref().cloned().unwrap_or_default(),
            pending_epochs: vec![],
        }
    }
}

fn apply_strategies(
    stream_id: u64,
    config: &Config,
    applicant: &PolicyApplicant,
    stream_info: &mut StreamInner,
) -> Vec<Command> {
    if let Some(policy) = stream_info.switch_policy {
        if let Some(cmd) = policy.apply(applicant, stream_info) {
            return vec![cmd];
        }
    }

    // stale request, promote it
    if applicant.epoch < stream_info.epoch {
        // The observer might lost heartbeat response, so here check and accept
        // the staled heartbeat request from current leader, and continue to promote
        // it for idempotent.
        info!("stream {} epoch {} leader {} promote observer {}, epoch: {}, by receiving staled heartbeat",
            stream_id, stream_info.epoch, stream_info.leader_desc(),
            applicant.observer_id, applicant.epoch);
        return vec![stream_info.gen_promote_cmd(&applicant.observer_id)];
    }

    // check leader
    let now = Instant::now();
    let select_new_leader = match &stream_info.leader {
        Some(observer_id) => {
            let observer_info = stream_info
                .observers
                .get(observer_id)
                .expect("stream must exists if it is a leader");
            // Leader might lost, need select new leader
            observer_info.last_heartbeat + config.heartbeat_timeout() <= now
        }
        None => true,
    };
    if select_new_leader {
        return vec![stream_info.elect_leader(applicant)];
    }
    vec![]
}
