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

use super::{Bucket, Stream};
use crate::{
    async_trait, Error, KernelUpdate, Result, ResultStream, Sequence, Version, VersionUpdate,
};

#[derive(Clone)]
pub struct Kernel {
    inner: Arc<Mutex<Inner>>,
    stream: Stream,
    bucket: Bucket,
}

struct Inner {
    current: Arc<Version>,
    updates: broadcast::Sender<Arc<VersionUpdate>>,
    last_sequence: Sequence,
}

impl Default for Kernel {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(1024);
        let inner = Inner {
            current: Arc::new(Version::default()),
            updates: tx,
            last_sequence: 0,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            stream: Stream::default(),
            bucket: Bucket::default(),
        }
    }
}

#[async_trait]
impl crate::Kernel for Kernel {
    type Bucket = Bucket;
    type Stream = Stream;

    async fn stream(&self) -> Result<Self::Stream> {
        Ok(self.stream.clone())
    }

    async fn bucket(&self) -> Result<Self::Bucket> {
        Ok(self.bucket.clone())
    }

    async fn install_update(&self, update: KernelUpdate) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.last_sequence += 1;
        let mut version_update = update.update;
        version_update.sequence = inner.last_sequence;
        // TODO: update current version
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
