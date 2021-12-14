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

use std::path::Path;

use engula_journal::grpc::Journal;
use engula_storage::grpc::Storage;

use crate::{
    file::Manifest as FileManifest, local::Kernel as LocalKernel, mem::Manifest as MemManifest,
    Result,
};

pub type Kernel<M> = LocalKernel<Journal, Storage, M>;
pub type MemKernel = Kernel<MemManifest>;

impl MemKernel {
    pub async fn open(journal_addr: &str, storage_addr: &str) -> Result<Self> {
        let journal = Journal::connect(journal_addr).await?;
        let storage = Storage::connect(storage_addr).await?;
        Self::init(journal, storage, MemManifest::default()).await
    }
}

pub type FileKernel = Kernel<FileManifest>;

impl FileKernel {
    pub async fn open<P: AsRef<Path>>(
        journal_endpoint: &str,
        storage_endpoint: &str,
        path: P,
    ) -> Result<Self> {
        let journal = Journal::connect(journal_endpoint).await?;
        let storage = Storage::connect(storage_endpoint).await?;
        let manifest = FileManifest::open(path.as_ref()).await;
        Self::init(journal, storage, manifest).await
    }
}
