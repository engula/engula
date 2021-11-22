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

use std::{
    collections::{hash_map, HashMap},
    sync::Arc,
};

use tokio::sync::Mutex;

use super::{
    bucket::MemObjectUploader,
    error::{Error, Result},
    object::MemObject,
};
use crate::{async_trait, Storage};

pub struct MemStorage {
    buckets: Mutex<HashMap<String, MemBucket>>,
}

impl MemStorage {
    async fn bucket(&self, name: &str) -> Result<MemBucket> {
        let buckets = self.buckets.lock().await;
        match buckets.get(name) {
            Some(bucket) => Ok(bucket.clone()),
            None => Err(Error::NotFound(format!("bucket '{}'", name))),
        }
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        MemStorage {
            buckets: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Storage<MemObject> for MemStorage {
    type ObjectUploader = MemObjectUploader;

    async fn create_bucket(&self, name: &str) -> Result<()> {
        let bucket = MemBucket::default();
        let mut buckets = self.buckets.lock().await;
        match buckets.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(bucket.clone());
                Ok(())
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

    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<MemObject> {
        let bucket = self.bucket(bucket_name).await?;
        let objects = bucket.objects.lock().await;
        match objects.get(object_name) {
            Some(object) => Ok(object.clone()),
            None => Err(Error::NotFound(format!("object '{}'", object_name))),
        }
    }

    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::ObjectUploader> {
        let bucket = self.bucket(bucket_name).await?;
        Ok(MemObjectUploader::new(
            object_name.to_owned(),
            bucket.objects,
        ))
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        let bucket = self.bucket(bucket_name).await?;
        let mut objects = bucket.objects.lock().await;
        match objects.remove(object_name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("object '{}'", object_name))),
        }
    }
}

type Objects = Arc<Mutex<HashMap<String, MemObject>>>;

#[derive(Clone)]
struct MemBucket {
    objects: Objects,
}

impl Default for MemBucket {
    fn default() -> Self {
        MemBucket {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
