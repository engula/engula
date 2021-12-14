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

use engula_journal::Error as JournalError;
use engula_storage::Error as StorageError;
use futures::TryStreamExt;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    async_trait, manifest::Manifest, Error, Journal, KernelUpdate, Result, ResultStream, Sequence,
    Storage, Version, VersionUpdate,
};

#[derive(Clone)]
pub struct Kernel<J: Journal, S: Storage, M: Manifest> {
    inner: Arc<Mutex<Inner>>,
    journal: J,
    storage: S,
    manifest: M,
}

struct Inner {
    current: Arc<Version>,
    updates: broadcast::Sender<Arc<VersionUpdate>>,
}

impl<J, S, M> Kernel<J, S, M>
where
    J: Journal,
    S: Storage,
    M: Manifest,
{
    pub async fn init(journal: J, storage: S, manifest: M) -> Result<Self> {
        let version = manifest.load_version().await?;
        let (updates, _) = broadcast::channel(1024);
        let inner = Inner {
            current: Arc::new(version),
            updates,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            journal,
            storage,
            manifest,
        })
    }
}

pub(crate) const DEFAULT_NAME: &str = "DEFAULT";

#[async_trait]
impl<J, S, M> crate::Kernel for Kernel<J, S, M>
where
    J: Journal,
    S: Storage,
    M: Manifest,
{
    type Bucket = S::Bucket;
    type Stream = J::Stream;

    async fn stream(&self) -> Result<Self::Stream> {
        match self.journal.stream(DEFAULT_NAME).await {
            Ok(stream) => Ok(stream),
            Err(JournalError::NotFound(_)) => {
                let stream = self.journal.create_stream(DEFAULT_NAME).await?;
                Ok(stream)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn bucket(&self) -> Result<Self::Bucket> {
        match self.storage.bucket(DEFAULT_NAME).await {
            Ok(bucket) => Ok(bucket),
            Err(StorageError::NotFound(_)) => {
                let bucket = self.storage.create_bucket(DEFAULT_NAME).await?;
                Ok(bucket)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn apply_update(&self, update: KernelUpdate) -> Result<()> {
        let mut inner = self.inner.lock().await;

        let mut version = (*inner.current).clone();
        let mut version_update = update.update;
        version_update.sequence = version.sequence + 1;
        version.update(&version_update);
        self.manifest.save_version(&version).await?;

        inner.current = Arc::new(version);
        inner
            .updates
            .send(Arc::new(version_update))
            .map_err(Error::unknown)?;
        Ok(())
    }

    async fn current_version(&self) -> Result<Arc<Version>> {
        let inner = self.inner.lock().await;
        Ok(inner.current.clone())
    }

    async fn version_updates(&self, _: Sequence) -> ResultStream<Arc<VersionUpdate>> {
        // TODO: handle sequence
        let inner = self.inner.lock().await;
        let stream = BroadcastStream::new(inner.updates.subscribe());
        Box::new(stream.map_err(Error::unknown))
    }
}
