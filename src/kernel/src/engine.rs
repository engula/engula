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

use engula_journal::Stream;
use engula_storage::{Object, ObjectUploader};

use crate::{async_trait, Result, ResultStream, Sequence, UpdateAction, Version, VersionUpdate};

#[async_trait]
pub trait Engine {
    async fn stream(&self, stream_name: &str) -> Result<Box<dyn Stream>>;

    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<Box<dyn Object>>;

    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Box<dyn ObjectUploader>>;

    async fn install_update(&self, update: EngineUpdate) -> Result<()>;

    async fn current_version(&self) -> Result<Arc<Version>>;

    async fn version_updates(&self, sequence: Sequence) -> ResultStream<Arc<VersionUpdate>>;
}

pub struct EngineUpdate {
    pub(crate) actions: Vec<UpdateAction>,
}

impl EngineUpdate {
    pub fn add_stream(&mut self, stream_name: String) -> &mut Self {
        let action = UpdateAction::AddStream(stream_name);
        self.actions.push(action);
        self
    }

    pub fn add_bucket(&mut self, bucket_name: String) -> &mut Self {
        let action = UpdateAction::AddBucket(bucket_name);
        self.actions.push(action);
        self
    }

    pub fn add_object(&mut self, bucket_name: String, object_name: String) -> &mut Self {
        let action = UpdateAction::AddObject(bucket_name, object_name);
        self.actions.push(action);
        self
    }

    pub fn delete_object(&mut self, bucket_name: String, object_name: String) -> &mut Self {
        let action = UpdateAction::DeleteObject(bucket_name, object_name);
        self.actions.push(action);
        self
    }
}
