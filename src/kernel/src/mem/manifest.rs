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

use tokio::sync::Mutex;

use crate::{async_trait, Result, Version};

#[derive(Clone)]
pub struct Manifest {
    current: Arc<Mutex<Version>>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            current: Arc::new(Mutex::new(Version::default())),
        }
    }
}

#[async_trait]
impl crate::manifest::Manifest for Manifest {
    async fn load_version(&self) -> Result<Version> {
        let current = self.current.lock().await;
        Ok(current.clone())
    }

    async fn save_version(&self, version: &Version) -> Result<()> {
        let mut current = self.current.lock().await;
        (*current) = version.clone();
        Ok(())
    }
}
