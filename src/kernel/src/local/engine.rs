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

use engula_journal::{Journal, Stream, Timestamp};
use engula_storage::{Object, ObjectUploader, Storage};

use crate::{async_trait, Engine, EngineAction, EngineUpdate, Result};

#[derive(Clone)]
pub struct LocalEngine<T: Timestamp> {
    journal: Arc<dyn Journal<T>>,
    storage: Arc<dyn Storage>,
}

impl<T: Timestamp> LocalEngine<T> {
    pub fn new(journal: Box<dyn Journal<T>>, storage: Box<dyn Storage>) -> LocalEngine<T> {
        LocalEngine {
            journal: Arc::from(journal),
            storage: Arc::from(storage),
        }
    }
}

#[async_trait]
impl<T: Timestamp> Engine<T> for LocalEngine<T> {
    async fn stream(&self, stream_name: &str) -> Result<Box<dyn Stream<T>>> {
        let stream = self.journal.stream(stream_name).await?;
        Ok(stream)
    }

    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<Box<dyn Object>> {
        let object = self.storage.object(bucket_name, object_name).await?;
        Ok(object)
    }

    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Box<dyn ObjectUploader>> {
        let uploader = self.storage.upload_object(bucket_name, object_name).await?;
        Ok(uploader)
    }

    async fn install_update(&self, update: EngineUpdate) -> Result<()> {
        for action in update.actions {
            self.take_action(action).await?;
        }
        Ok(())
    }
}

impl<T: Timestamp> LocalEngine<T> {
    async fn take_action(&self, action: EngineAction) -> Result<()> {
        match action {
            EngineAction::AddStream(name) => self.create_stream(&name).await,
            EngineAction::AddBucket(name) => self.create_bucket(&name).await,
            _ => Ok(()),
        }
    }

    async fn create_stream(&self, name: &str) -> Result<()> {
        self.journal.create_stream(name).await?;
        Ok(())
    }

    async fn create_bucket(&self, name: &str) -> Result<()> {
        self.storage.create_bucket(name).await?;
        Ok(())
    }
}
