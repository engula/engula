// Copyright 2021 The Engula Authors.
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

use std::sync::Arc;

use engula_journal::Stream;
use engula_storage::Bucket;

use crate::{async_trait, Result, ResultStream, Sequence, UpdateAction, Version, VersionUpdate};

#[async_trait]
pub trait Kernel {
    type Stream: Stream;
    type Bucket: Bucket;

    async fn stream(&self) -> Result<Self::Stream>;

    async fn bucket(&self) -> Result<Self::Bucket>;

    async fn install_update(&self, update: KernelUpdate) -> Result<()>;

    async fn current_version(&self) -> Result<Arc<Version>>;

    async fn version_updates(&self, sequence: Sequence) -> ResultStream<Arc<VersionUpdate>>;
}

pub struct KernelUpdate {
    pub(crate) actions: Vec<UpdateAction>,
}

impl KernelUpdate {
    pub fn add_object(&mut self, object_name: String) -> &mut Self {
        let action = UpdateAction::AddObject(object_name);
        self.actions.push(action);
        self
    }

    pub fn delete_object(&mut self, object_name: String) -> &mut Self {
        let action = UpdateAction::DeleteObject(object_name);
        self.actions.push(action);
        self
    }
}
