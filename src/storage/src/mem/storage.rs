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

use super::{object::MemObject, uploader::MemObjectUploader};
use crate::{async_trait, Error, Object, ObjectUploader, Result, Storage};

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
impl Storage for MemStorage {
    async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        let bucket = HashMap::new();
        let mut buckets = self.buckets.lock().await;
        match buckets.entry(bucket_name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(bucket);
                Ok(())
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("bucket '{}'", ent.key())))
            }
        }
    }

    async fn delete_bucket(&self, bucket_name: &str) -> Result<()> {
        let mut buckets = self.buckets.lock().await;
        match buckets.remove(bucket_name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("bucket '{}'", bucket_name))),
        }
    }

    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<Box<dyn Object>> {
        let buckets = self.buckets.lock().await;
        let objects = match buckets.get(bucket_name) {
            Some(objects) => Ok(objects),
            None => Err(Error::NotFound(format!("bucket '{}'", bucket_name))),
        }?;
        match objects.get(object_name) {
            Some(object) => Ok(Box::new(object.clone())),
            None => Err(Error::NotFound(format!("object '{}'", object_name))),
        }
    }

    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Box<dyn ObjectUploader>> {
        let uploader = MemObjectUploader::new(
            self.buckets.clone(),
            bucket_name.to_owned(),
            object_name.to_owned(),
        );
        Ok(Box::new(uploader))
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
