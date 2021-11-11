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

use std::collections::HashMap;

use futures::{future, stream};
use tokio::sync::Mutex;

use super::bucket::MemBucket;
use crate::{async_trait, BoxStream, Bucket, Error, Result, Storage};

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
impl Storage for MemStorage {
    async fn bucket(&self, name: &str) -> Result<Box<dyn Bucket>> {
        let buckets = self.buckets.lock().await;
        match buckets.get(name) {
            Some(bucket) => Ok(Box::new(bucket.clone())),
            None => Err(Error::NotFound(format!("bucket '{}'", name))),
        }
    }

    async fn list_buckets(&self) -> BoxStream<Result<Vec<String>>> {
        let buckets = self.buckets.lock().await;
        let bucket_names = buckets.keys().cloned().collect::<Vec<String>>();
        Box::new(stream::once(future::ok(bucket_names)))
    }

    async fn create_bucket(&self, name: &str) -> Result<Box<dyn Bucket>> {
        let bucket = MemBucket::new();
        let mut buckets = self.buckets.lock().await;
        match buckets.try_insert(name.to_owned(), bucket.clone()) {
            Ok(_) => Ok(Box::new(bucket)),
            Err(_) => Err(Error::AlreadyExists(format!("bucket '{}'", name))),
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
