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

use futures::TryStreamExt;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    async_trait, manifest::Manifest, Bucket, Error, KernelUpdate, Result, ResultStream, Sequence,
    Stream, Version, VersionUpdate,
};

#[derive(Clone)]
pub struct Kernel<S: Stream, B: Bucket, M: Manifest> {
    inner: Arc<Mutex<Inner>>,
    stream: S,
    bucket: B,
    manifest: M,
}

struct Inner {
    current: Arc<Version>,
    updates: broadcast::Sender<Arc<VersionUpdate>>,
}

impl<S, B, M> Kernel<S, B, M>
where
    S: Stream,
    B: Bucket,
    M: Manifest,
{
    pub async fn init(stream: S, bucket: B, manifest: M) -> Result<Self> {
        let version = manifest.load_version().await?;
        let (updates, _) = broadcast::channel(1024);
        let inner = Inner {
            current: Arc::new(version),
            updates,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            stream,
            bucket,
            manifest,
        })
    }
}

#[async_trait]
impl<S, B, M> crate::Kernel for Kernel<S, B, M>
where
    S: Stream,
    B: Bucket,
    M: Manifest,
{
    type Bucket = B;
    type Stream = S;

    async fn stream(&self) -> Result<Self::Stream> {
        Ok(self.stream.clone())
    }

    async fn bucket(&self) -> Result<Self::Bucket> {
        Ok(self.bucket.clone())
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
