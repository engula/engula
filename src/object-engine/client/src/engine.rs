// Copyright 2022 The Engula Authors.
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

use std::path::PathBuf;

use object_engine_master::{proto::*, FileStore};

use crate::{Master, Result, Tenant};

#[derive(Clone)]
pub struct Engine {
    master: Master,
    file_store: FileStore,
}

impl Engine {
    async fn new(master: Master) -> Result<Self> {
        let file_store = master.file_store().await?;
        Ok(Self { master, file_store })
    }

    /// Opens a local engine.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let master = Master::open(path).await?;
        Self::new(master).await
    }

    /// Connects to a remote engine service.
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let master = Master::connect(url).await?;
        Self::new(master).await
    }

    pub fn tenant(&self, name: &str) -> Tenant {
        let file_tenant = self.file_store.tenant(name);
        Tenant::new(name.to_owned(), self.master.clone(), file_tenant.into())
    }

    pub async fn create_tenant(&self, name: &str) -> Result<TenantDesc> {
        let desc = TenantDesc {
            name: name.to_owned(),
        };
        self.master.create_tenant(desc).await
    }
}
