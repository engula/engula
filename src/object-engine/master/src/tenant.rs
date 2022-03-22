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

use object_engine_lsmstore::{Tenant as VersionTenant, VersionEditFile};
use tokio::sync::Mutex;

use crate::{proto::*, Bucket, Error, Result};

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<TenantInner>,
}

impl Tenant {
    pub fn new(name: String, options: TenantOptions, versions_tenant: VersionTenant) -> Self {
        let inner = TenantInner::new(name, options, versions_tenant);
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub async fn desc(&self) -> TenantDesc {
        self.inner.desc().await
    }

    pub async fn bucket(&self, name: &str) -> Result<Bucket> {
        self.inner.bucket(name).await
    }

    pub(crate) async fn create_bucket(&self, name: &str, options: BucketOptions) -> Result<Bucket> {
        self.inner.create_bucket(name, options).await
    }

    pub async fn add_files(&self, files: Vec<VersionEditFile>) -> Result<()> {
        self.inner.versions_tenant.add_files(files).await
    }

    pub async fn get_next_file_num(&self, count: u64) -> Result<Vec<u64>> {
        self.inner.versions_tenant.get_next_file_nums(count).await
    }
}

struct TenantInner {
    name: String,
    options: TenantOptions,
    buckets: Mutex<HashMap<String, Bucket>>,
    versions_tenant: VersionTenant,
}

impl TenantInner {
    fn new(name: String, options: TenantOptions, versions_tenant: VersionTenant) -> Self {
        Self {
            name,
            options,
            buckets: Mutex::new(HashMap::new()),
            versions_tenant,
        }
    }

    async fn desc(&self) -> TenantDesc {
        let buckets = self.buckets.lock().await;
        let properties = TenantProperties {
            num_buckets: buckets.len() as u64,
        };
        TenantDesc {
            name: self.name.clone(),
            options: Some(self.options.clone()),
            properties: Some(properties),
        }
    }

    async fn bucket(&self, name: &str) -> Result<Bucket> {
        let buckets = self.buckets.lock().await;
        buckets
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("bucket {}", name)))
    }

    async fn create_bucket(&self, name: &str, options: BucketOptions) -> Result<Bucket> {
        let mut buckets = self.buckets.lock().await;
        if buckets.contains_key(name) {
            return Err(Error::AlreadyExists(format!("bucket {}", name)));
        }
        self.versions_tenant.create_bucket(name).await?;
        let versioin_bucket = self.versions_tenant.bucket(name).await?;
        let bucket = Bucket::new(name.to_owned(), self.name.clone(), options, versioin_bucket);
        buckets.insert(name.to_owned(), bucket.clone());
        Ok(bucket)
    }
}
