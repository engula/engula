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

use tokio::sync::{broadcast, Mutex};

use crate::{
    async_trait, manifest::Manifest, Error, Journal, KernelUpdate, Result, Storage, Timestamp,
    Version, VersionUpdate,
};

#[derive(Clone)]
pub struct Kernel<J, S, M> {
    inner: Arc<Mutex<Inner>>,
    journal: J,
    storage: S,
    manifest: M,
}

struct Inner {
    current: Version,
    updates: broadcast::Sender<VersionUpdate>,
}

impl<J, S, M> Kernel<J, S, M>
where
    M: Manifest,
{
    pub async fn init(journal: J, storage: S, manifest: M) -> Result<Self> {
        let current = manifest.load_version().await?;
        let (updates, _) = broadcast::channel(1024);
        let inner = Inner { current, updates };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            journal,
            storage,
            manifest,
        })
    }
}

#[async_trait]
impl<T, J, S, M> crate::Kernel<T> for Kernel<J, S, M>
where
    T: Timestamp,
    J: Journal<T>,
    S: Storage,
    M: Manifest,
{
    type RandomReader = S::RandomReader;
    type SequentialWriter = S::SequentialWriter;
    type StreamReader = J::StreamReader;
    type StreamWriter = J::StreamWriter;
    type VersionUpdateStream = VersionUpdateStream;

    async fn new_stream_reader(&self, stream_name: &str) -> Result<Self::StreamReader> {
        let reader = self.journal.new_stream_reader(stream_name).await?;
        Ok(reader)
    }

    async fn new_stream_writer(&self, stream_name: &str) -> Result<Self::StreamWriter> {
        let writer = self.journal.new_stream_writer(stream_name).await?;
        Ok(writer)
    }

    async fn new_random_object_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader> {
        let reader = self
            .storage
            .new_random_reader(bucket_name, object_name)
            .await?;
        Ok(reader)
    }

    async fn new_sequential_object_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialWriter> {
        let writer = self
            .storage
            .new_sequential_writer(bucket_name, object_name)
            .await?;
        Ok(writer)
    }

    async fn apply_update(&self, update: KernelUpdate) -> Result<()> {
        let mut inner = self.inner.lock().await;

        let mut version = inner.current.clone();
        let mut version_update = update.update;
        version_update.sequence = version.sequence + 1;
        version.update(&version_update)?;
        self.manifest.save_version(&version).await?;

        inner.current = version;
        inner.updates.send(version_update).map_err(Error::unknown)?;
        Ok(())
    }

    async fn current_version(&self) -> Result<Version> {
        let inner = self.inner.lock().await;
        Ok(inner.current.clone())
    }

    async fn subscribe_version_updates(&self) -> Result<Self::VersionUpdateStream> {
        let inner = self.inner.lock().await;
        let stream = VersionUpdateStream::new(inner.updates.subscribe());
        Ok(stream)
    }
}

pub struct VersionUpdateStream {
    rx: broadcast::Receiver<VersionUpdate>,
}

impl VersionUpdateStream {
    fn new(rx: broadcast::Receiver<VersionUpdate>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl crate::VersionUpdateStream for VersionUpdateStream {
    async fn next(&mut self) -> Result<VersionUpdate> {
        self.rx.recv().await.map_err(Error::unknown)
    }
}
