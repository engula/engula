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

use std::{collections::HashMap, sync::Arc};

use stream_engine_proto::*;
use tokio::sync::Mutex;

use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Master {
    inner: Arc<Mutex<MasterInner>>,
}

struct MasterInner {
    next_id: u64,
    tenants: HashMap<String, Tenant>,
}

impl Master {
    pub fn new() -> Self {
        let inner = MasterInner {
            next_id: 1,
            tenants: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn tenant(&self, name: &str) -> Result<Tenant> {
        let inner = self.inner.lock().await;
        inner
            .tenants
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("tenant {}", name)))
    }

    pub async fn create_tenant(&self, mut desc: TenantDesc) -> Result<TenantDesc> {
        let mut inner = self.inner.lock().await;
        if inner.tenants.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("tenant {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        let db = Tenant::new(desc.clone());
        inner.tenants.insert(desc.name.clone(), db);
        Ok(desc)
    }
}

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<Mutex<TenantInner>>,
}

struct TenantInner {
    desc: TenantDesc,
    next_id: u64,
    streams: HashMap<String, StreamDesc>,
}

impl Tenant {
    fn new(desc: TenantDesc) -> Self {
        let inner = TenantInner {
            desc,
            next_id: 1,
            streams: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> TenantDesc {
        self.inner.lock().await.desc.clone()
    }

    pub async fn stream(&self, name: &str) -> Result<StreamDesc> {
        let inner = self.inner.lock().await;
        inner
            .streams
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("stream {}", name)))
    }

    pub async fn create_stream(&self, mut desc: StreamDesc) -> Result<StreamDesc> {
        let mut inner = self.inner.lock().await;
        if inner.streams.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("stream {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        desc.parent_id = inner.desc.id;
        inner.streams.insert(desc.name.clone(), desc.clone());
        Ok(desc)
    }
}
