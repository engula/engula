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

use std::{collections::HashMap, sync::Arc};

use futures::stream;
use tokio::sync::Mutex;

use super::object::MemObject;
use crate::{async_trait, BoxStream, Bucket, Error, Object, ObjectUploader, Result};

type Objects = Arc<Mutex<HashMap<String, MemObject>>>;

#[derive(Clone)]
pub struct MemBucket {
    objects: Objects,
}

impl MemBucket {
    pub fn new() -> MemBucket {
        MemBucket {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Bucket for MemBucket {
    async fn object(&self, name: &str) -> Result<Box<dyn Object>> {
        let objects = self.objects.lock().await;
        match objects.get(name) {
            Some(object) => Ok(Box::new(object.clone())),
            None => Err(Error::NotFound(format!("object '{}'", name))),
        }
    }

    async fn list_objects(&self) -> BoxStream<Result<String>> {
        let objects = self.objects.lock().await;
        let object_names = objects
            .keys()
            .cloned()
            .map(Ok)
            .collect::<Vec<Result<String>>>();
        Box::new(stream::iter(object_names))
    }

    async fn upload_object(&self, name: &str) -> Box<dyn ObjectUploader> {
        Box::new(MemObjectUploader::new(
            name.to_owned(),
            self.objects.clone(),
        ))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        let mut objects = self.objects.lock().await;
        match objects.remove(name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("object '{}'", name))),
        }
    }
}

struct MemObjectUploader {
    name: String,
    data: Vec<u8>,
    objects: Objects,
}

impl MemObjectUploader {
    fn new(name: String, objects: Objects) -> MemObjectUploader {
        MemObjectUploader {
            name,
            data: Vec::new(),
            objects,
        }
    }
}

#[async_trait]
impl ObjectUploader for MemObjectUploader {
    async fn write(&mut self, buf: &[u8]) {
        self.data.extend_from_slice(buf);
    }

    async fn finish(self) -> Result<usize> {
        let object = MemObject::new(self.data);
        let mut objects = self.objects.lock().await;
        match objects.try_insert(self.name, object) {
            Ok(v) => Ok(v.len()),
            Err(err) => Err(Error::AlreadyExists(format!(
                "object '{}'",
                err.entry.key()
            ))),
        }
    }
}
