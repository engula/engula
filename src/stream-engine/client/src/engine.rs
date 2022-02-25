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

use crate::{
    master::{Master, Tenant},
    Result,
};

#[derive(Clone)]
pub struct Engine {
    master: Master,
}

impl Engine {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        Ok(Engine {
            master: Master::new(url).await?,
        })
    }

    #[inline(always)]
    pub fn tenant(&self, name: &str) -> Tenant {
        self.master.tenant(name)
    }

    #[inline(always)]
    pub async fn create_tenant(&self, name: &str) -> Result<Tenant> {
        self.master.create_tenant(name).await
    }

    #[inline(always)]
    pub async fn delete_tenant(&self, name: &str) -> Result<()> {
        self.master.delete_tenant(name).await
    }
}
