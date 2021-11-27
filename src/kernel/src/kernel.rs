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

use crate::{Version, VersionUpdate};

use async_trait::async_trait;
use engula_journal::Stream;
use engula_storage::Object;

#[async_trait]
pub trait Kernel {
    type Error: std::error::Error;
    type Stream: Stream;
    type Object: Object;
    type VersionUpdateStream: futures::Stream<Item = Result<VersionUpdate, Self::Error>>;

    fn stream(&self, name: &str) -> Result<Self::Stream, Self::Error>;

    fn object(&self, bucket_name: &str, object_name: &str) -> Result<Self::Object, Self::Error>;

    fn install(&self, edit: KernelEdit) -> Result<(), Self::Error>;

    /// Returns the current version.
    ///
    /// TODO: for a large data set, a version can be large too (Gigabytes). Consider breaking a version into multiple parts in that case.
    async fn current_version(&self) -> Result<Version, Self::Error>;

    /// Returns a stream of version updates.
    async fn version_updates(&self) -> Result<Self::VersionUpdateStream, Self::Error>;
}

pub struct KernelEdit {}

impl KernelEdit {
    pub fn create_stream(&mut self, name: String) -> &mut Self {
        self
    }

    pub fn create_bucket(&mut self, name: String) -> &mut Self {
        self
    }

    pub fn insert_object(&mut self, bucket: String, object: String) -> &mut Self {
        self
    }

    pub fn delete_object(&mut self, bucket: String, object: String) -> &mut Self {
        self
    }
}
