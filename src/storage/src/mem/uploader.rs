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

use super::{
    error::{Error, Result},
    object::MemObject,
    storage::Buckets,
};
use crate::{async_trait, ObjectUploader};

pub struct MemObjectUploader {
    bucket_name: String,
    object_name: String,
    data: Vec<u8>,
    buckets: Buckets,
}

impl MemObjectUploader {
    pub fn new(
        bucket_name: impl Into<String>,
        object_name: impl Into<String>,
        buckets: Buckets,
    ) -> MemObjectUploader {
        MemObjectUploader {
            bucket_name: bucket_name.into(),
            object_name: object_name.into(),
            data: Vec::new(),
            buckets,
        }
    }
}

#[async_trait]
impl ObjectUploader for MemObjectUploader {
    type Error = Error;

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.data.extend_from_slice(buf);
        Ok(())
    }

    async fn finish(self) -> Result<usize> {
        let len = self.data.len();
        let object = MemObject::new(self.data);

        let mut buckets = self.buckets.lock().await;
        let objects = match buckets.get_mut(&self.bucket_name) {
            Some(objects) => Ok(objects),
            None => Err(Error::NotFound(format!("bucket '{}'", &self.bucket_name))),
        }?;

        match objects.entry(self.object_name.clone()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(object.clone());
                Ok(len)
            }
            hash_map::Entry::Occupied(_) => Err(Error::AlreadyExists(format!(
                "object '{}'",
                self.object_name
            ))),
        }
    }
}
