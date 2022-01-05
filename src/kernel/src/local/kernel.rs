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

use tokio::sync::{broadcast, Mutex};

use crate::{async_trait, Error, Journal, KernelUpdate, Result, Sequence, Storage, Timestamp};

pub struct Kernel<J, S> {
    inner: Mutex<Inner>,
    journal: J,
    storage: S,
}

struct Inner {
    sequence: Sequence,
    update_tx: broadcast::Sender<KernelUpdate>,
}

impl<J, S> Kernel<J, S> {
    pub async fn init(journal: J, storage: S) -> Result<Self> {
        let (update_tx, _) = broadcast::channel(1024);
        let inner = Inner {
            sequence: 0,
            update_tx,
        };
        Ok(Self {
            inner: Mutex::new(inner),
            journal,
            storage,
        })
    }
}

#[async_trait]
impl<T, J, S> crate::Kernel<T> for Kernel<J, S>
where
    T: Timestamp,
    J: Journal<T>,
    S: Storage,
{
    type KernelUpdateReader = KernelUpdateReader;
    type RandomObjectReader = S::RandomReader;
    type SequentialObjectWriter = S::SequentialWriter;
    type StreamReader = J::StreamReader;
    type StreamWriter = J::StreamWriter;

    async fn apply_update(&self, mut update: KernelUpdate) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.sequence += 1;
        update.sequence = inner.sequence;
        inner.update_tx.send(update).map_err(Error::unknown)?;
        Ok(())
    }

    async fn new_update_reader(&self) -> Result<Self::KernelUpdateReader> {
        let inner = self.inner.lock().await;
        let reader = KernelUpdateReader::new(inner.update_tx.subscribe());
        Ok(reader)
    }

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
    ) -> Result<Self::RandomObjectReader> {
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
    ) -> Result<Self::SequentialObjectWriter> {
        let writer = self
            .storage
            .new_sequential_writer(bucket_name, object_name)
            .await?;
        Ok(writer)
    }
}

pub struct KernelUpdateReader {
    rx: broadcast::Receiver<KernelUpdate>,
}

impl KernelUpdateReader {
    fn new(rx: broadcast::Receiver<KernelUpdate>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl crate::KernelUpdateReader for KernelUpdateReader {
    async fn try_next(&mut self) -> Result<Option<KernelUpdate>> {
        match self.rx.try_recv() {
            Ok(v) => Ok(Some(v)),
            Err(broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(err) => Err(Error::unknown(err)),
        }
    }

    async fn wait_next(&mut self) -> Result<KernelUpdate> {
        self.rx.recv().await.map_err(Error::unknown)
    }
}
