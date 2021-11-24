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
    error::{Error, Result},
    object::MemObject,
    uploader::MemObjectUploader,
};
use crate::{async_trait, Storage};

type Objects = HashMap<String, MemObject>;

pub type Buckets = Arc<Mutex<HashMap<String, Objects>>>;

pub struct MemStorage {
    buckets: Buckets,
}

impl Default for MemStorage {
    fn default() -> Self {
        MemStorage {
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Storage<MemObject> for MemStorage {
    type ObjectUploader = MemObjectUploader;

    async fn create_bucket(&self, name: &str) -> Result<()> {
        let bucket = HashMap::new();
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
        let buckets = self.buckets.lock().await;
        let objects = match buckets.get(bucket_name) {
            Some(objects) => Ok(objects),
            None => Err(Error::NotFound(format!("bucket '{}'", bucket_name))),
        }?;
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
        Ok(MemObjectUploader::new(
            bucket_name.to_owned(),
            object_name.to_owned(),
            self.buckets.clone(),
        ))
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        let mut buckets = self.buckets.lock().await;
        let objects = match buckets.get_mut(bucket_name) {
            Some(objects) => Ok(objects),
            None => Err(Error::NotFound(format!("bucket '{}'", bucket_name))),
        }?;
        match objects.remove(object_name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("object '{}'", object_name))),
        }
    }
}
