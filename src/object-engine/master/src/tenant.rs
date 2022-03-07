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

use tokio::sync::Mutex;

use crate::{fs::FileTenant, proto::*, Bucket, Error, Result};

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<Mutex<TenantInner>>,
}

impl Tenant {
    pub fn new(desc: TenantDesc, file_tenant: FileTenant) -> Self {
        let inner = TenantInner::new(desc, file_tenant);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> TenantDesc {
        let inner = self.inner.lock().await;
        inner.desc.clone()
    }

    pub async fn bucket(&self, name: &str) -> Result<Bucket> {
        let inner = self.inner.lock().await;
        inner.bucket(name)
    }

    pub async fn create_bucket(&self, desc: BucketDesc) -> Result<BucketDesc> {
        let mut inner = self.inner.lock().await;
        inner.create_bucket(desc).await
    }
}

struct TenantInner {
    desc: TenantDesc,
    buckets: HashMap<String, Bucket>,
    file_tenant: FileTenant,
}

impl TenantInner {
    fn new(desc: TenantDesc, file_tenant: FileTenant) -> Self {
        Self {
            desc,
            buckets: HashMap::new(),
            file_tenant,
        }
    }

    fn bucket(&self, name: &str) -> Result<Bucket> {
        self.buckets
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("bucket {}", name)))
    }

    async fn create_bucket(&mut self, mut desc: BucketDesc) -> Result<BucketDesc> {
        if self.buckets.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("bucket {}", desc.name)));
        }
        desc.tenant = self.desc.name.clone();
        let file_bucket = self.file_tenant.create_bucket(&desc.name).await?;
        let bucket = Bucket::new(desc.clone(), file_bucket.into());
        self.buckets.insert(desc.name.clone(), bucket);
        Ok(desc)
    }
}
