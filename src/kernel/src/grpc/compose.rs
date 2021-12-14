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

use std::{path::Path, sync::Arc};

use engula_journal::grpc::{Journal, Stream};
use engula_storage::grpc::{Bucket, Storage};

use super::proto::*;
use crate::{
    async_trait, file::Manifest as FileManifest, local::Kernel as LocalKernel, manifest::Manifest,
    mem::Manifest as MemManifest, KernelUpdate, Result, ResultStream, Sequence,
};

#[derive(Clone)]
pub struct Kernel<M: Manifest> {
    journal_addr: String,
    storage_addr: String,
    kernel: LocalKernel<Journal, Storage, M>,
}

impl<M> Kernel<M>
where
    M: Manifest,
{
    pub(crate) async fn init(journal_addr: &str, storage_addr: &str, manifest: M) -> Result<Self> {
        let journal = Journal::connect(journal_addr).await?;
        let storage = Storage::connect(storage_addr).await?;
        let kernel = LocalKernel::init(journal, storage, manifest).await?;
        Ok(Kernel {
            journal_addr: journal_addr.to_owned(),
            storage_addr: storage_addr.to_owned(),
            kernel,
        })
    }

    pub(crate) fn get_journal_addr(&self) -> &str {
        &self.journal_addr
    }

    pub(crate) fn get_storage_addr(&self) -> &str {
        &self.storage_addr
    }
}

#[async_trait]
impl<M> crate::Kernel for Kernel<M>
where
    M: Manifest,
{
    type Bucket = Bucket;
    type Stream = Stream;

    /// Returns a journal stream.
    async fn stream(&self) -> Result<Self::Stream> {
        self.kernel.stream().await
    }

    /// Returns a storage bucket.
    async fn bucket(&self) -> Result<Self::Bucket> {
        self.kernel.bucket().await
    }

    /// Applies a kernel update.
    async fn apply_update(&self, update: KernelUpdate) -> Result<()> {
        self.kernel.apply_update(update).await
    }

    /// Returns the current version.
    async fn current_version(&self) -> Result<Arc<Version>> {
        self.kernel.current_version().await
    }

    /// Returns a stream of version updates since a given sequence (inclusive).
    async fn version_updates(&self, sequence: Sequence) -> ResultStream<Arc<VersionUpdate>> {
        self.kernel.version_updates(sequence).await
    }
}

pub type MemKernel = Kernel<MemManifest>;

impl MemKernel {
    pub async fn open(journal_addr: &str, storage_addr: &str) -> Result<Self> {
        Self::init(journal_addr, storage_addr, MemManifest::default()).await
    }
}

pub type FileKernel = Kernel<FileManifest>;

impl FileKernel {
    pub async fn open<P: AsRef<Path>>(
        journal_addr: &str,
        storage_addr: &str,
        path: P,
    ) -> Result<Self> {
        let manifest = FileManifest::open(path.as_ref()).await;
        Self::init(journal_addr, storage_addr, manifest).await
    }
}
