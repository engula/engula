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
use raft::prelude::Snapshot;

use super::{SnapManager, SNAP_DATA};
use crate::{
    raftgroup::{applier::Applier, metrics::*, StateMachine},
    record_latency,
};

pub fn apply_snapshot<M: StateMachine>(
    replica_id: u64,
    snap_mgr: &SnapManager,
    applier: &mut Applier<M>,
    snapshot: &Snapshot,
) {
    record_latency!(take_apply_snapshot_metrics());
    let snap_id = &snapshot.data;
    let snap_info = snap_mgr
        .lock_snap(replica_id, snap_id)
        .expect("The snapshot should does not be gc before apply");
    // TODO(walter) check snapshot data integrity.
    let snap_dir = snap_info.base_dir.join(SNAP_DATA);
    applier
        .apply_snapshot(&snap_dir)
        .expect("apply snapshot must success, because the data integrity might broken");
}
