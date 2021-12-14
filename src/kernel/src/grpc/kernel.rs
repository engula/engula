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

use engula_journal::{grpc as grpc_journal, Error as JournalError, Journal};
use engula_storage::{grpc as grpc_storage, Error as StorageError, Storage};
use futures::StreamExt;

use super::{client::Client, proto::*};
use crate::{async_trait, Error, KernelUpdate, Result, ResultStream, Sequence};

pub(crate) const DEFAULT_STREAM: &str = "DEFAULT";
pub(crate) const DEFAULT_BUCKET: &str = "DEFAULT";

#[derive(Clone)]
pub struct Kernel {
    client: Client,
    journal: grpc_journal::Journal,
    storage: grpc_storage::Storage,
}

impl Kernel {
    pub async fn connect(addr: &str) -> Result<Kernel> {
        let client = Client::connect(addr).await?;
        let resp = client.place_lookup(PlaceLookupRequest {}).await?;
        let journal = grpc_journal::Journal::connect(&resp.journal_address).await?;
        let storage = grpc_storage::Storage::connect(&resp.storage_address).await?;
        Ok(Kernel {
            client,
            journal,
            storage,
        })
    }
}

#[async_trait]
impl crate::Kernel for Kernel {
    type Bucket = grpc_storage::Bucket;
    type Stream = grpc_journal::Stream;

    /// Returns a journal stream.
    async fn stream(&self) -> Result<Self::Stream> {
        match self.journal.stream(DEFAULT_STREAM).await {
            Ok(stream) => Ok(stream),
            Err(JournalError::NotFound(_)) => {
                Ok(self.journal.create_stream(DEFAULT_STREAM).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Returns a storage bucket.
    async fn bucket(&self) -> Result<Self::Bucket> {
        match self.storage.bucket(DEFAULT_BUCKET).await {
            Ok(bucket) => Ok(bucket),
            Err(StorageError::NotFound(_)) => {
                Ok(self.storage.create_bucket(DEFAULT_BUCKET).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Applies a kernel update.
    async fn apply_update(&self, update: KernelUpdate) -> Result<()> {
        let input = ApplyUpdateRequest {
            version_update: Some(update.update),
        };
        self.client.apply_update(input).await?;
        Ok(())
    }

    /// Returns the current version.
    async fn current_version(&self) -> Result<Arc<Version>> {
        let input = CurrentVersionRequest {};
        let resp = self.client.current_version(input).await?;
        let version = resp
            .version
            .ok_or_else(|| Error::Internal("CurrentVersionResponse::version is none".into()))?;
        Ok(Arc::new(version))
    }

    /// Returns a stream of version updates since a given sequence (inclusive).
    async fn version_updates(&self, sequence: Sequence) -> ResultStream<Arc<VersionUpdate>> {
        let input = VersionUpdatesRequest { sequence };
        match self.client.version_updates(input).await {
            Ok(output) => Box::new(output.map(|result| match result {
                Ok(resp) => Ok(Arc::new(resp.version_update.ok_or_else(|| {
                    Error::Internal("VersionUpdatesResponse::version_update is none".into())
                })?)),
                Err(status) => Err(status.into()),
            })),
            Err(e) => Box::new(futures::stream::once(futures::future::err(e))),
        }
    }
}
