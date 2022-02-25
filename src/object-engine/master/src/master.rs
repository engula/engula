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

use object_engine_proto::*;
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
    buckets: HashMap<String, BucketDesc>,
}

impl Tenant {
    fn new(desc: TenantDesc) -> Self {
        let inner = TenantInner {
            desc,
            next_id: 1,
            buckets: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> TenantDesc {
        self.inner.lock().await.desc.clone()
    }

    pub async fn bucket(&self, name: &str) -> Result<BucketDesc> {
        let inner = self.inner.lock().await;
        inner
            .buckets
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("bucket {}", name)))
    }

    pub async fn create_bucket(&self, mut desc: BucketDesc) -> Result<BucketDesc> {
        let mut inner = self.inner.lock().await;
        if inner.buckets.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("bucket {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        desc.parent_id = inner.desc.id;
        inner.buckets.insert(desc.name.clone(), desc.clone());
        Ok(desc)
    }
}
