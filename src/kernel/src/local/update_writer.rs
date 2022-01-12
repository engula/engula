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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use engula_journal::Journal;
use engula_storage::Storage;
use tokio::sync::broadcast::Sender;

use crate::{async_trait, Error, KernelUpdate, Result, Sequence, UpdateEvent};

pub struct UpdateWriter<J, S> {
    journal: Arc<J>,
    storage: Arc<S>,
    sequence: Arc<AtomicU64>,
    tx: Sender<UpdateEvent>,
}

impl<J, S> UpdateWriter<J, S>
where
    J: Journal,
    S: Storage,
{
    pub fn new(
        journal: Arc<J>,
        storage: Arc<S>,
        sequence: Arc<AtomicU64>,
        tx: Sender<UpdateEvent>,
    ) -> Self {
        Self {
            journal,
            storage,
            sequence,
            tx,
        }
    }
}

#[async_trait]
impl<J, S> crate::UpdateWriter for UpdateWriter<J, S>
where
    J: Journal + Send + Sync,
    S: Storage + Send + Sync,
{
    async fn append(&mut self, update: KernelUpdate) -> Result<Sequence> {
        for stream in &update.add_streams {
            self.journal.create_stream(stream).await?;
        }
        for stream in &update.remove_streams {
            self.journal.delete_stream(stream).await?;
        }
        for bucket in &update.add_buckets {
            self.storage.create_bucket(bucket).await?;
        }
        for bucket in &update.remove_buckets {
            self.storage.delete_bucket(bucket).await?;
        }
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        self.tx.send((sequence, update)).map_err(Error::unknown)?;
        Ok(sequence)
    }

    async fn release(&mut self, _: Sequence) -> Result<()> {
        // TODO
        Ok(())
    }
}
