use std::{collections::HashMap, sync::Arc};

use bytes::BufMut;
use futures::{
    future,
    stream::{self, Stream},
};
use tokio::sync::Mutex;

use super::mem_object::MemObject;
use crate::{async_trait, Error, Result, StorageBucket, StorageObject, UploadObject};

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
impl StorageBucket for MemBucket {
    async fn object(&self, name: &str) -> Result<Box<dyn StorageObject>> {
        let objects = self.objects.lock().await;
        if let Some(object) = objects.get(name) {
            Ok(Box::new(object.clone()))
        } else {
            Err(Error::NotFound(format!("object '{}'", name)))
        }
    }

    async fn list_objects(&self) -> Box<dyn Stream<Item = Result<Vec<String>>>> {
        let objects = self.objects.lock().await;
        let object_names = objects.keys().cloned().collect::<Vec<String>>();
        Box::new(stream::once(future::ok(object_names)))
    }

    async fn upload_object(&self, name: &str) -> Box<dyn UploadObject> {
        Box::new(ObjectUploader::new(name.to_owned(), self.objects.clone()))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        let mut objects = self.objects.lock().await;
        match objects.remove(name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("object '{}'", name))),
        }
    }
}

struct ObjectUploader {
    name: String,
    data: Vec<u8>,
    objects: Objects,
}

impl ObjectUploader {
    fn new(name: String, objects: Objects) -> ObjectUploader {
        ObjectUploader {
            name,
            data: Vec::new(),
            objects,
        }
    }
}

#[async_trait]
impl UploadObject for ObjectUploader {
    async fn write(&mut self, buf: &[u8]) {
        self.data.put_slice(buf);
    }

    async fn finish(self) -> Result<usize> {
        let object = MemObject::new(self.data);
        let mut objects = self.objects.lock().await;
        match objects.try_insert(self.name, object) {
            Ok(v) => Ok(v.len()),
            Err(err) => Err(Error::AlreadyExist(format!("object '{}'", err.entry.key()))),
        }
    }
}
