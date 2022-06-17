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

use std::path::Path;

use engula_api::server::v1::{ChangeReplicas, GroupDesc};

use crate::{
    serverpb::v1::{EvalResult, ApplyState},
    Result,
};

/// A helper structure to used to access the internal field of entries.
pub enum ApplyEntry {
    Empty,
    ConfigChange { change_replicas: ChangeReplicas },
    Proposal { eval_result: EvalResult },
}

/// An abstraction of finate state machine. It is used by `RaftNode` to apply entries.
pub trait StateMachine: Send {
    fn apply(&mut self, index: u64, term: u64, entry: ApplyEntry) -> Result<()>;

    // TODO(walter) define snapshot
    fn apply_snapshot(&mut self) -> Result<()>;

    fn snapshot_builder(&self) -> Box<dyn SnapshotBuilder>;

    fn descriptor(&self) -> GroupDesc;

    /// Return the latest index which persisted in disk.
    fn flushed_index(&self) -> u64;
}

/// An abstraction of snapshot generation.
#[crate::async_trait]
pub trait SnapshotBuilder: Send + Sync {
    /// Stable this checkpoint to `base_dir`, and returns applied index and group descriptor.
    async fn checkpoint(&self, base_dir: &Path) -> Result<(ApplyState, GroupDesc)>;
}
