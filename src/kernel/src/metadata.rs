// Copyright 2021 The Engula Authors.
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

pub mod v1 {
    tonic::include_proto!("engula.metadata.v1");
}

use std::collections::hash_map;

pub use v1::{BucketDesc, BucketUpdate, Version, VersionUpdate};

use crate::{Error, Result};

/// An increasing number to order versions.
pub type Sequence = u64;

impl Version {
    pub fn update(&mut self, update: &VersionUpdate) -> Result<()> {
        self.sequence = update.sequence;

        for (name, meta) in &update.put_meta {
            self.meta.insert(name.clone(), meta.clone());
        }
        for name in &update.remove_meta {
            self.meta.remove(name);
        }

        for (name, meta) in &update.add_streams {
            match self.streams.entry(name.clone()) {
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(meta.clone());
                }
                hash_map::Entry::Occupied(ent) => {
                    return Err(Error::AlreadyExists(format!("stream '{}'", ent.key())));
                }
            }
        }
        for name in &update.remove_streams {
            self.streams.remove(name);
        }

        for (name, meta) in &update.add_buckets {
            match self.buckets.entry(name.clone()) {
                hash_map::Entry::Vacant(ent) => {
                    let bucket = BucketDesc {
                        meta: meta.to_owned(),
                        ..Default::default()
                    };
                    ent.insert(bucket);
                }
                hash_map::Entry::Occupied(ent) => {
                    return Err(Error::AlreadyExists(format!("bucket '{}'", ent.key())));
                }
            }
        }
        for name in &update.remove_buckets {
            self.buckets.remove(name);
        }

        for (name, update) in &update.update_buckets {
            let bucket = self
                .buckets
                .entry(name.clone())
                .or_insert_with(|| BucketDesc::default());
            for (object_name, object_meta) in &update.add_objects {
                match bucket.objects.entry(object_name.clone()) {
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(object_meta.clone());
                    }
                    hash_map::Entry::Occupied(ent) => {
                        return Err(Error::AlreadyExists(format!("object '{}'", ent.key())));
                    }
                }
            }
            for object_name in &update.remove_objects {
                bucket.objects.remove(object_name);
            }
        }

        Ok(())
    }
}
