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

use std::collections::hash_map;

use super::{object::MemObject, storage::Buckets};
use crate::{async_trait, Error, ObjectUploader, Result};

pub struct MemObjectUploader {
    buckets: Buckets,
    bucket_name: String,
    object_name: String,
    object_data: Vec<u8>,
}

impl MemObjectUploader {
    pub fn new(
        buckets: Buckets,
        bucket_name: impl Into<String>,
        object_name: impl Into<String>,
    ) -> MemObjectUploader {
        MemObjectUploader {
            buckets,
            bucket_name: bucket_name.into(),
            object_name: object_name.into(),
            object_data: Vec::new(),
        }
    }
}

#[async_trait]
impl ObjectUploader for MemObjectUploader {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.object_data.extend_from_slice(buf);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<usize> {
        let len = self.object_data.len();
        let object = MemObject::new(self.object_data);

        let mut buckets = self.buckets.lock().await;
        let objects = match buckets.get_mut(&self.bucket_name) {
            Some(objects) => Ok(objects),
            None => Err(Error::NotFound(format!("bucket '{}'", self.bucket_name))),
        }?;
        match objects.entry(self.object_name) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(object.clone());
                Ok(len)
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("object '{}'", ent.key())))
            }
        }
    }
}
