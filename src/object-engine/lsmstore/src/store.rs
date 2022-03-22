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
    collections::{hash_map, BTreeSet, HashMap},
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

use object_engine_filestore::{SequentialWrite, Store as FileStore};
use tokio::sync::Mutex;

use crate::{
    iterator::{LevelIter, ManifestIter, MergingIterator},
    versions::{proto::*, BucketVersion, OrdByUpperBound, VersionSet},
    Error, Key, Result, TableReader, ValueType, VersionEditBuilder, VersionEditFile,
};

#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    base_dir: PathBuf,
    version_sets: HashMap<String, VersionSet>, // tenant -> VersionSet
    local_store: Option<Arc<dyn FileStore>>,
    external_store: Arc<dyn FileStore>,
}

impl Store {
    pub async fn new(
        base_dir: impl Into<PathBuf>,
        local_store: Option<Arc<dyn FileStore>>,
        external_store: Arc<dyn FileStore>,
    ) -> Result<Self> {
        let inner = Inner {
            base_dir: base_dir.into(),
            version_sets: HashMap::new(),
            local_store,
            external_store,
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
                ent.insert(VersionSet::open(base_dir.join(name)).await?);
            }
        }
        Ok(Tenant {
            tenant: name.to_owned(),
            inner: self.inner.clone(),
        })
    }

    pub async fn create_tenant(&self, _name: &str) -> Result<()> {
        // TODO: save tenant info in some place.
        Ok(())
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

    pub async fn create_bucket(&self, name: &str) -> Result<()> {
        let inner = self.inner.lock().await;

        inner
            .external_store
            .tenant(&self.tenant)
            .create_bucket(name)
            .await?;

        let version_tenant = inner
            .version_sets
            .get(&self.tenant)
            .ok_or_else(|| Error::NotFound(format!("tenant {}", &self.tenant)))?;
        version_tenant
            .log_and_apply(
                VersionEditBuilder::default()
                    .add_buckets(vec![version_edit::Bucket {
                        name: name.to_owned(),
                    }])
                    .build(),
            )
            .await?;
        Ok(())
    }

    pub async fn add_files(&self, files: Vec<VersionEditFile>) -> Result<()> {
        let inner = self.inner.lock().await;

        let ve = VersionEditBuilder::default()
            .add_files(files.to_owned())
            .build();
        let tenant = inner.version_sets.get(&self.tenant).unwrap();
        tenant.log_and_apply(ve).await?;

        if inner.local_store.is_some() {
            for f in &files {
                let bucket = inner
                    .local_store
                    .as_ref()
                    .unwrap()
                    .tenant(&self.tenant)
                    .bucket(&f.bucket);

                let r = bucket.new_sequential_reader(&f.name).await?;
                let w = bucket.new_sequential_writer(&f.name).await?;
                object_engine_filestore::copy_all(r, w).await?;
            }
        }

        Ok(())
    }
}

pub struct Bucket {
    tenant: String,
    bucket: String,
    inner: Arc<Mutex<Inner>>,
}

impl Bucket {
    pub async fn get(&self, id: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().await;
        let vs = inner
            .version_sets
            .get(&self.tenant)
            .ok_or_else(|| Error::NotFound(format!("tenant {}", &self.tenant)))?;
        let current = vs
            .current_version()
            .await
            .bucket_version(&self.bucket)
            .await?;
        inner
            .get(current, &self.tenant, &self.bucket, id, u64::MAX)
            .await
    }

    pub async fn iter(&self) -> Result<MergingIterator> {
        let inner = self.inner.lock().await;
        let vs = inner
            .version_sets
            .get(&self.tenant)
            .ok_or_else(|| Error::NotFound(format!("tenant {}", &self.tenant)))?;
        let current = vs
            .current_version()
            .await
            .bucket_version(&self.bucket)
            .await?;

        let merge_iter = inner
            .get_merge_iter(&self.tenant, &self.bucket, current)
            .await?;

        Ok(merge_iter)
    }

    pub async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn SequentialWrite>> {
        let inner = self.inner.lock().await;
        inner
            .external_store
            .tenant(&self.tenant)
            .bucket(&self.bucket)
            .new_sequential_writer(name)
            .await
    }
}

impl Inner {
    fn get_store(&self) -> Arc<dyn FileStore> {
        if self.local_store.is_none() {
            self.external_store.clone()
        } else {
            self.local_store.as_ref().unwrap().clone()
        }
    }

    async fn get(
        &self,
        ver: BucketVersion,
        tenant: &str,
        bucket: &str,
        id: &[u8],
        snapshot_ts: u64,
    ) -> Result<Option<Vec<u8>>> {
        let key = Key::encode_to_vec(id, u64::MAX, ValueType::Put);
        for l in ver.l0_level.iter().rev() {
            let r = self
                .get_store()
                .tenant(tenant)
                .bucket(bucket)
                .new_random_reader(&l.name)
                .await?;

            let mut iter = TableReader::open(r.into(), l.file_size as usize)
                .await?
                .iter();
            iter.seek(key.as_slice().into()).await?;
            while iter.valid() {
                let k = iter.key();
                if k.id() != id {
                    break;
                }
                if k.ts() > snapshot_ts {
                    continue;
                }
                match k.tp() {
                    ValueType::Put => return Ok(Some(iter.value().to_owned())),
                    ValueType::Delete => return Ok(None),
                    ValueType::Merge => todo!(),
                }
            }
        }

        for l in ver.non_l0_levels {
            let level_file = l.iter();
            let local_store = self.get_store();
            let mut iter = LevelIter::new(tenant, bucket, level_file, local_store).await?;
            iter.seek(key.as_slice().into()).await?;
            while iter.valid() {
                let k = iter.key();
                if k.is_none() {
                    break;
                }
                let k = k.unwrap();
                if k.id() != key {
                    break;
                }
                if k.ts() > snapshot_ts {
                    continue;
                }
                match k.tp() {
                    ValueType::Put => return Ok(Some(iter.value().to_owned())),
                    ValueType::Delete => return Ok(None),
                    ValueType::Merge => todo!(),
                }
            }
        }

        Ok(None)
    }

    async fn get_merge_iter(
        &self,
        tenant: &str,
        bucket: &str,
        version: BucketVersion,
    ) -> Result<MergingIterator> {
        let mut level_iters = Vec::new();

        for l0_file in version.l0_level.iter().rev() {
            let mut fs = BTreeSet::new();
            fs.insert(OrdByUpperBound(l0_file.deref().to_owned()));
            let manifest_file_iter = ManifestIter::new(fs);
            let local_store = self.get_store();
            let merge_iter =
                LevelIter::new(tenant, bucket, manifest_file_iter, local_store).await?;
            level_iters.push(merge_iter);
        }

        for level_files in version.non_l0_levels {
            let manifest_file_iter = level_files.iter();
            let local_store = self.get_store();
            let merge_iter =
                LevelIter::new(tenant, bucket, manifest_file_iter, local_store).await?;
            level_iters.push(merge_iter);
        }

        Ok(MergingIterator::new(level_iters, None))
    }
}
