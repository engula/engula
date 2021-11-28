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

use engula_journal::Stream;
use engula_storage::{Object, ObjectUploader};

use crate::{async_trait, EngineUpdate, Result};

#[async_trait]
pub trait Engine {
    async fn stream(&self, stream_name: impl Into<String>) -> Result<Box<dyn Stream>>;

    async fn object(
        &self,
        bucket_name: impl Into<String>,
        object_name: impl Into<String>,
    ) -> Result<Box<dyn Object>>;

    async fn upload_object(
        &self,
        bucket_name: impl Into<String>,
        object_name: impl Into<String>,
    ) -> Result<Box<dyn ObjectUploader>>;

    async fn install_update(&self, update: EngineUpdate) -> Result<()>;
}
