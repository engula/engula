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

use async_trait::async_trait;
use engula_futures::stream::batch::ResultStream;

use super::{ReplicaMeta, SegmentMeta};
use crate::{Error, Result};

/// A journal server instance abstraction.  The master needs to operate the
/// journal server instance to get replica state, place or recycle replicas.
#[async_trait]
pub(super) trait Instance {
    /// Places a segment replica on this instance.
    async fn place(&self, segment_meta: &SegmentMeta) -> Result<()>;

    /// Delete a useless replica.
    async fn delete(&self, stream_name: &str, epoch: u32) -> Result<()>;

    /// Get meta of replicas on this instance.
    async fn get_replicas(&self) -> Result<Vec<ReplicaMeta>>;
}

#[async_trait]
pub(super) trait Orchestrator {
    type Instance: Instance;
    type InstanceLister: ResultStream<Elem = Self::Instance, Error = Error>;

    async fn list_instances(&self) -> Result<Self::InstanceLister>;

    async fn provision_instance(&self) -> Result<Self::Instance>;

    async fn deprovision_instance(&self, instance: Self::Instance) -> Result<()>;
}
