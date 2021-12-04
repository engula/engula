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

use crate::{async_trait, Bucket, Result, ResultStream, Sequence, Stream, Version, VersionUpdate};

/// An interface to interact with a kernel.
#[async_trait]
pub trait Kernel: Clone + Send + Sync + 'static {
    type Stream: Stream;
    type Bucket: Bucket;

    /// Returns a journal stream.
    async fn stream(&self) -> Result<Self::Stream>;

    /// Returns a storage bucket.
    async fn bucket(&self) -> Result<Self::Bucket>;

    /// Installs a kernel update.
    async fn install_update(&self, update: KernelUpdate) -> Result<()>;

    /// Returns the current version.
    async fn current_version(&self) -> Result<Arc<Version>>;

    /// Returns a stream of version updates since a given sequence (inclusive).
    async fn version_updates(&self, sequence: Sequence) -> ResultStream<Arc<VersionUpdate>>;
}

#[derive(Default)]
pub struct KernelUpdate {
    pub(crate) update: VersionUpdate,
}

impl KernelUpdate {
    pub fn set_meta(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.update.set_meta.insert(key.into(), value.into());
        self
    }

    pub fn delete_meta(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.update.delete_meta.push(key.into());
        self
    }

    pub fn add_object(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.add_objects.push(name.into());
        self
    }

    pub fn delete_object(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.delete_objects.push(name.into());
        self
    }
}
