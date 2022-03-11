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

use std::{
    collections::{hash_map, HashMap},
    path::PathBuf,
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{versions::VersionSet, BucketIter, Error, Result};

pub struct Store {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    base_dir: PathBuf,
    version_sets: HashMap<String, VersionSet>, // tenant -> VersionSet
}

impl Store {
    pub async fn new(base_dir: impl Into<PathBuf>) -> Result<Self> {
        let inner = Inner {
            base_dir: base_dir.into(),
            version_sets: HashMap::new(),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub async fn tenant(&self, name: &str) -> Result<Tenant> {
        let mut inner = self.inner.lock().await;
        let base_dir = inner.base_dir.to_owned();
        match inner.version_sets.entry(name.to_owned()) {
            hash_map::Entry::Occupied(_) => {}
            hash_map::Entry::Vacant(ent) => {
                ent.insert(VersionSet::open(base_dir, name).await?);
            }
        }
        Ok(Tenant {
            tenant: name.to_owned(),
            inner: self.inner.clone(),
        })
    }
}

pub struct Tenant {
    tenant: String,
    inner: Arc<Mutex<Inner>>,
}

impl Tenant {
    pub async fn bucket(&self, name: &str) -> Result<Bucket> {
        Ok(Bucket {
            tenant: self.tenant.to_owned(),
            bucket: name.to_owned(),
            inner: self.inner.clone(),
        })
    }
}

pub struct Bucket {
    tenant: String,
    bucket: String,
    inner: Arc<Mutex<Inner>>,
}

impl Bucket {
    pub async fn get(&self, _id: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().await;
        let vs = inner
            .version_sets
            .get(&self.tenant)
            .ok_or_else(|| Error::NotFound(format!("tenant {}", &self.tenant)))?;
        let current = vs.current_version().await;
        let _current = current.bucket_version(&self.bucket).await?;

        todo!();
    }

    pub fn iter(&self) -> BucketIter {
        BucketIter {}
    }
}
