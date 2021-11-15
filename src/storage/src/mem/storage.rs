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

use std::collections::{hash_map, HashMap};

use tokio::sync::Mutex;

use super::{
    bucket::MemBucket,
    error::{Error, Result},
    object::MemObject,
};
use crate::{async_trait, Storage};

pub struct MemStorage {
    buckets: Mutex<HashMap<String, MemBucket>>,
}

impl Default for MemStorage {
    fn default() -> Self {
        MemStorage {
            buckets: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Storage<MemObject, MemBucket> for MemStorage {
    async fn bucket(&self, name: &str) -> Result<MemBucket> {
        let buckets = self.buckets.lock().await;
        match buckets.get(name) {
            Some(bucket) => Ok(bucket.clone()),
            None => Err(Error::NotFound(format!("bucket '{}'", name))),
        }
    }

    async fn create_bucket(&self, name: &str) -> Result<MemBucket> {
        let bucket = MemBucket::default();
        let mut buckets = self.buckets.lock().await;
        match buckets.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(bucket.clone());
                Ok(bucket)
            }
            hash_map::Entry::Occupied(_) => Err(Error::AlreadyExists(format!("bucket '{}'", name))),
        }
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let mut buckets = self.buckets.lock().await;
        match buckets.remove(name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("bucket '{}'", name))),
        }
    }
}
