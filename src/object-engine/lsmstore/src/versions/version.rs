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
    collections::{btree_map, BTreeMap},
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{versions::proto::*, *};

const NUM_LEVELS: usize = 7;

type Key = Vec<u8>;

#[derive(Clone)]
pub struct FileMetadata {
    pub name: String,
    pub bucket: String,
    pub level: u32,
    pub smallest: Key,
    pub largest: Key,
}

type LevelFiles = BTreeMap<Key, FileMetadata>; // smallest_key => FileMetadata
type BucketLevels = Vec<LevelFiles>; // level -> level files

#[derive(Clone)]
pub struct BucketVersion {
    pub levels: BucketLevels,                  // level + key -> file (for read)
    pub files: BTreeMap<String, FileMetadata>, // filename -> file (for delete)
}

impl Default for BucketVersion {
    fn default() -> Self {
        let levels = (0..NUM_LEVELS)
            .into_iter()
            .map(|_| BTreeMap::new())
            .collect();
        let files = BTreeMap::new();
        Self { levels, files }
    }
}

#[derive(Default, Clone)]
pub struct Version {
    pub inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
pub struct Inner {
    pub buckets: BTreeMap<String, BucketVersion>, // bucket => levels;
}

impl Version {
    pub async fn apply(&mut self, ve: &VersionEdit) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.apply(ve).await?;
        Ok(())
    }

    pub async fn generate_snapshot(&self, next_file_num: u64) -> VersionEdit {
        let inner = self.inner.lock().await;
        inner.generate_snapshot(next_file_num).await
    }

    pub async fn bucket_version(&self, bucket: &str) -> Result<BucketVersion> {
        let inner = self.inner.lock().await;
        Ok(inner
            .buckets
            .get(bucket)
            .ok_or_else(|| Error::NotFound(format!("bucket {}", bucket)))?
            .to_owned())
    }
}

impl Inner {
    async fn apply(&mut self, ve: &VersionEdit) -> Result<()> {
        for add_bucket in &ve.add_buckets {
            match self.buckets.entry(add_bucket.name.to_owned()) {
                btree_map::Entry::Vacant(ent) => Some(ent.insert(BucketVersion::default())),
                btree_map::Entry::Occupied(_) => None,
            };
        }
        for bucket in &ve.remove_buckets {
            self.buckets.remove(bucket);
        }

        for add_file in &ve.add_files {
            if let Some(bucket) = self.buckets.get_mut(&add_file.bucket) {
                let file_meta = FileMetadata {
                    name: add_file.name.to_owned(),
                    bucket: add_file.bucket.to_owned(),
                    level: add_file.level,
                    smallest: add_file.smallest.to_owned(),
                    largest: add_file.largest.to_owned(),
                };

                match bucket.files.entry(add_file.name.to_owned()) {
                    btree_map::Entry::Vacant(ent) => {
                        ent.insert(file_meta.to_owned());
                    }
                    btree_map::Entry::Occupied(_) => {
                        return Err(Error::AlreadyExists(format!(
                            "bucket {}, file {}",
                            &add_file.bucket, &add_file.name
                        )))
                    }
                };
                let files = bucket
                    .levels
                    .get_mut(add_file.level as usize)
                    .ok_or_else(|| Error::Internal("current version not found".to_string()))?;
                match files.entry(add_file.smallest.to_owned()) {
                    btree_map::Entry::Vacant(ent) => {
                        ent.insert(file_meta);
                    }
                    btree_map::Entry::Occupied(_) => {
                        return Err(Error::AlreadyExists(format!(
                            "bucket {}, level {}, key {:?}",
                            &add_file.bucket, &add_file.level, &add_file.smallest
                        )))
                    }
                }
            } else {
                return Err(Error::NotFound(format!("bucket {}", &add_file.bucket)));
            }
        }
        for file in &ve.remove_files {
            if let Some(bucket) = self.buckets.get_mut(&file.bucket) {
                let removed = bucket.files.remove(&file.name);
                if let Some(f) = removed {
                    bucket.levels[f.level as usize].remove(&f.smallest);
                }
            }
        }

        Ok(())
    }

    async fn generate_snapshot(&self, next_file_num: u64) -> VersionEdit {
        let mut b = VersionEditBuilder::default();
        let mut buckets = Vec::new();
        for (name, bucket) in &self.buckets {
            buckets.push(version_edit::Bucket {
                name: name.to_owned(),
            });
            for file in bucket.files.values() {
                b.add_files(vec![version_edit::File {
                    range_id: 1,
                    bucket: file.bucket.to_owned(),
                    name: file.name.to_owned(),
                    level: file.level,
                    smallest: file.smallest.to_owned(),
                    largest: file.largest.to_owned(),
                }]);
            }
        }
        b.set_next_file_num(next_file_num);
        b.build()
    }
}
