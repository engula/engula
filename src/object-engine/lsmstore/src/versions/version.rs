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
    cmp::Ordering,
    collections::{btree_map, hash_map, BTreeMap, BTreeSet, HashMap},
    ops::Deref,
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{iterator::ManifestIter, versions::proto::*, *};

const NUM_LEVELS: usize = 7;

#[derive(Clone, Default)]
pub struct FileMetadata {
    pub name: String,
    pub bucket: String,
    pub tenant: String,
    pub level: u32,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
    pub file_size: u64,
}

#[derive(Clone, Default)]
pub struct LevelFiles<T>
where
    T: Ord + Clone + PartialOrd + PartialEq + Deref + From<FileMetadata>,
{
    pub files: BTreeSet<T>, // ordered FileMetadata
}

impl LevelFiles<OrdByUpperBound> {
    pub fn iter(&self) -> ManifestIter {
        ManifestIter::new(self.files.clone())
    }
}

#[derive(Clone)]
pub struct BucketVersion {
    pub l0_level: Vec<FileMetadata>,
    pub non_l0_levels: Vec<LevelFiles<OrdByUpperBound>>,
    pub files: HashMap<String, FileMetadata>, // filename -> file (for delete)
}

impl Default for BucketVersion {
    fn default() -> Self {
        let non_l0_levels = (1..NUM_LEVELS)
            .into_iter()
            .map(|_| LevelFiles::default())
            .collect();
        Self {
            non_l0_levels,
            files: HashMap::new(),
            l0_level: Vec::new(),
        }
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
                    tenant: add_file.tenant.to_owned(),
                    level: add_file.level,
                    lower_bound: add_file.lower_bound.to_owned(),
                    upper_bound: add_file.upper_bound.to_owned(),
                    file_size: add_file.file_size.to_owned(),
                };

                match bucket.files.entry(add_file.name.to_owned()) {
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(file_meta.to_owned());
                    }
                    hash_map::Entry::Occupied(_) => {
                        return Err(Error::AlreadyExists(format!(
                            "bucket {}, file {}",
                            &add_file.bucket, &add_file.name
                        )))
                    }
                };

                let has_dup = if add_file.level == 0 {
                    bucket.l0_level.push(file_meta);
                    false
                } else {
                    let level_files = bucket
                        .non_l0_levels
                        .get_mut((add_file.level - 1) as usize)
                        .ok_or_else(|| Error::Internal("current version not found".to_string()))?;
                    !level_files.files.insert(OrdByUpperBound(file_meta))
                };
                if has_dup {
                    return Err(Error::AlreadyExists(format!(
                        "bucket {}, level {}, key {:?}",
                        &add_file.bucket, &add_file.level, &add_file.lower_bound
                    )));
                }
            } else {
                return Err(Error::NotFound(format!("bucket {}", &add_file.bucket)));
            }
        }
        for file in &ve.remove_files {
            if let Some(bucket) = self.buckets.get_mut(&file.bucket) {
                let removed = bucket.files.remove(&file.name);
                if let Some(f) = removed {
                    if f.level == 0 {
                        bucket.l0_level.retain(|e| e.lower_bound != f.lower_bound)
                    } else {
                        bucket.non_l0_levels[(f.level - 1) as usize].files.remove(
                            &OrdByUpperBound(FileMetadata {
                                lower_bound: f.lower_bound,
                                ..Default::default()
                            }),
                        );
                    }
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
                    tenant: file.tenant.to_owned(),
                    name: file.name.to_owned(),
                    level: file.level,
                    lower_bound: file.lower_bound.to_owned(),
                    upper_bound: file.upper_bound.to_owned(),
                    file_size: file.file_size.to_owned(),
                }]);
            }
        }
        b.set_next_file_num(next_file_num);
        b.build()
    }
}

#[derive(Clone, Default)]
pub struct OrdByUpperBound(pub FileMetadata);

impl From<FileMetadata> for OrdByUpperBound {
    fn from(m: FileMetadata) -> Self {
        Self(m)
    }
}

impl Deref for OrdByUpperBound {
    type Target = FileMetadata;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Ord for OrdByUpperBound {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        Key::from(self.0.upper_bound.as_slice()).cmp(&Key::from(other.0.upper_bound.as_slice()))
    }
}

impl PartialOrd for OrdByUpperBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrdByUpperBound {
    fn eq(&self, other: &Self) -> bool {
        self.0.lower_bound == other.0.lower_bound
    }
}

impl Eq for OrdByUpperBound {}
