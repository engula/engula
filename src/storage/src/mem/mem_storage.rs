use std::collections::HashMap;

use futures::{
    future,
    stream::{self, Stream},
};
use tokio::sync::Mutex;

use super::mem_bucket::MemBucket;
use crate::{async_trait, Error, Result, Storage, StorageBucket};

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
    async fn bucket(&self, name: &str) -> Result<Box<dyn StorageBucket>> {
        let buckets = self.buckets.lock().await;
        if let Some(bucket) = buckets.get(name) {
            Ok(Box::new(bucket.clone()))
        } else {
            Err(Error::NotFound(format!("bucket '{}'", name)))
        }
    }

    async fn list_buckets(&self) -> Box<dyn Stream<Item = Result<Vec<String>>>> {
        let buckets = self.buckets.lock().await;
        let bucket_names = buckets.keys().cloned().collect::<Vec<String>>();
        Box::new(stream::once(future::ok(bucket_names)))
    }

    async fn create_bucket(&self, name: &str) -> Result<Box<dyn StorageBucket>> {
        let bucket = MemBucket::new();
        let mut buckets = self.buckets.lock().await;
        match buckets.try_insert(name.to_owned(), bucket.clone()) {
            Ok(_) => Ok(Box::new(bucket)),
            Err(_) => Err(Error::AlreadyExist(format!("bucket '{}'", name))),
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
