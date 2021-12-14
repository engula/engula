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

use engula_journal::{grpc as grpc_journal, Error as JournalError, Journal};
use engula_storage::{grpc as grpc_storage, Error as StorageError, Storage};

use crate::{
    file::Manifest as FileManifest,
    local::{Kernel as LocalKernel, DEFAULT_NAME},
    manifest::Manifest,
    mem::Manifest as MemManifest,
    Result,
};

pub type Kernel<M> = LocalKernel<grpc_journal::Journal, grpc_storage::Storage, M>;

async fn create_default_stream(journal: &impl Journal) -> Result<()> {
    match journal.create_stream(DEFAULT_NAME).await {
        Err(JournalError::AlreadyExists(_)) => Ok(()),
        Ok(_) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

async fn create_default_bucket(storage: &impl Storage) -> Result<()> {
    match storage.create_bucket(DEFAULT_NAME).await {
        Err(StorageError::AlreadyExists(_)) => Ok(()),
        Ok(_) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

async fn create_kernel<M: Manifest>(
    journal_endpoint: &str,
    storage_endpoint: &str,
    manifest: M,
) -> Result<Kernel<M>> {
    let journal = grpc_journal::Journal::connect(journal_endpoint).await?;
    let storage = grpc_storage::Storage::connect(storage_endpoint).await?;

    // HACK: Create default stream & bucket here to avoid manipulating the stream or
    // bucket from `Kernel::stream` or `Kernel::bucket` result not found.
    // See https://github.com/engula/engula/issues/194 for details.
    create_default_stream(&journal).await?;
    create_default_bucket(&storage).await?;
    Kernel::init(journal, storage, manifest).await
}

pub type MemKernel = Kernel<MemManifest>;

impl MemKernel {
    pub async fn open(journal_endpoint: &str, storage_endpoint: &str) -> Result<Self> {
        create_kernel(journal_endpoint, storage_endpoint, MemManifest::default()).await
    }
}

pub type FileKernel = Kernel<FileManifest>;

impl FileKernel {
    pub async fn open<P: AsRef<Path>>(
        journal_endpoint: &str,
        storage_endpoint: &str,
        path: P,
    ) -> Result<Self> {
        let manifest = FileManifest::open(path.as_ref()).await;
        create_kernel(journal_endpoint, storage_endpoint, manifest).await
    }
}
