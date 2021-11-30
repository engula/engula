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

use super::bucket::Bucket;
use crate::{async_trait, Error, Result};

pub struct Storage {
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl crate::Storage for Storage {
    type Bucket = Bucket;

    async fn bucket(&self, name: &str) -> Result<Self::Bucket> {
        let buckets = self.buckets.lock().await;
        match buckets.get(name) {
            Some(bucket) => Ok(bucket.clone()),
            None => Err(Error::NotFound(format!("bucket '{}'", name))),
        }
    }

    async fn create_bucket(&self, name: &str) -> Result<Self::Bucket> {
        let bucket = Bucket::default();
        let mut buckets = self.buckets.lock().await;
        match buckets.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(bucket.clone());
                Ok(bucket)
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("bucket '{}'", ent.key())))
            }
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
