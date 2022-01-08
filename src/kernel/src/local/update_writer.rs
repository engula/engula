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

use tokio::sync::broadcast::Sender;

use crate::{async_trait, Error, KernelUpdate, Result, Sequence, UpdateEvent};

pub struct UpdateWriter {
    sequence: Arc<AtomicU64>,
    tx: Sender<UpdateEvent>,
}

impl UpdateWriter {
    pub fn new(sequence: Arc<AtomicU64>, tx: Sender<UpdateEvent>) -> Self {
        Self { sequence, tx }
    }
}

#[async_trait]
impl crate::UpdateWriter for UpdateWriter {
    async fn append(&mut self, update: KernelUpdate) -> Result<Sequence> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.tx.send((sequence, update)).map_err(Error::unknown)?;
        Ok(sequence)
    }

    async fn release(&mut self, _: Sequence) -> Result<()> {
        // TODO
        Ok(())
    }
}
