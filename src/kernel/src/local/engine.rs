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

use engula_journal::{Journal, Stream};
use engula_storage::{Object, ObjectUploader, Storage};
use futures::TryStreamExt;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    async_trait, Engine, EngineUpdate, Error, Result, ResultStream, Sequence, UpdateAction,
    Version, VersionUpdate,
};

#[derive(Clone)]
pub struct LocalEngine {
    journal: Arc<dyn Journal>,
    storage: Arc<dyn Storage>,
    vset: Arc<Mutex<VersionSet>>,
}

struct VersionSet {
    current: Arc<Version>,
    updates: broadcast::Sender<Arc<VersionUpdate>>,
    last_sequence: Sequence,
}

impl LocalEngine {
    pub fn new(journal: Box<dyn Journal>, storage: Box<dyn Storage>) -> LocalEngine {
        let (tx, _) = broadcast::channel(1024);
        let vset = VersionSet {
            current: Arc::new(Version::default()),
            updates: tx,
            last_sequence: 0,
        };
        LocalEngine {
            journal: Arc::from(journal),
            storage: Arc::from(storage),
            vset: Arc::new(Mutex::new(vset)),
        }
    }
}

#[async_trait]
impl Engine for LocalEngine {
    async fn stream(&self, stream_name: &str) -> Result<Box<dyn Stream>> {
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
        for action in &update.actions {
            self.take_action(action).await?;
        }
        let mut vset = self.vset.lock().await;
        vset.last_sequence += 1;
        let version_update = Arc::new(VersionUpdate {
            sequence: vset.last_sequence,
            actions: update.actions,
        });
        vset.updates.send(version_update).map_err(Error::unknown)?;
        Ok(())
    }

    async fn current_version(&self) -> Result<Arc<Version>> {
        let vset = self.vset.lock().await;
        Ok(vset.current.clone())
    }

    async fn version_updates(&self, _: Sequence) -> ResultStream<Arc<VersionUpdate>> {
        // TODO: handle sequence
        let vset = self.vset.lock().await;
        let stream = BroadcastStream::new(vset.updates.subscribe());
        Box::new(stream.map_err(Error::unknown))
    }
}

impl LocalEngine {
    async fn take_action(&self, action: &UpdateAction) -> Result<()> {
        match action {
            UpdateAction::AddStream(name) => self.create_stream(name).await,
            UpdateAction::AddBucket(name) => self.create_bucket(name).await,
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
