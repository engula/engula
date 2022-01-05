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
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use engula_futures::io::{RandomRead, SequentialWrite};
use futures::ready;
use tokio::sync::Mutex;

use crate::{async_trait, Error, Result};

type Object = Arc<Vec<u8>>;
type Bucket = Arc<Mutex<HashMap<String, Object>>>;

#[derive(Clone)]
pub struct Storage {
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Storage {
    async fn bucket(&self, bucket_name: &str) -> Option<Bucket> {
        let buckets = self.buckets.lock().await;
        buckets.get(bucket_name).cloned()
    }
}

#[async_trait]
impl crate::Storage for Storage {
    type RandomReader = RandomReader;
    type SequentialWriter = SequentialWriter;

    async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        let mut buckets = self.buckets.lock().await;
        match buckets.entry(bucket_name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                let bucket = Arc::new(Mutex::new(HashMap::new()));
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

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        if let Some(bucket) = self.bucket(bucket_name).await {
            match bucket.lock().await.remove(object_name) {
                Some(_) => Ok(()),
                None => Err(Error::NotFound(format!("object '{}'", object_name))),
            }
        } else {
            Err(Error::NotFound(format!("bucket '{}'", bucket_name)))
        }
    }

    async fn new_random_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader> {
        if let Some(bucket) = self.bucket(bucket_name).await {
            match bucket.lock().await.get(object_name) {
                Some(object) => Ok(RandomReader::new(object.clone())),
                None => Err(Error::NotFound(format!("object '{}'", object_name))),
            }
        } else {
            Err(Error::NotFound(format!("bucket '{}'", bucket_name)))
        }
    }

    async fn new_sequential_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialWriter> {
        if let Some(bucket) = self.bucket(bucket_name).await {
            Ok(SequentialWriter::new(bucket, object_name.to_owned()))
        } else {
            Err(Error::NotFound(format!("bucket '{}'", bucket_name)))
        }
    }
}

pub struct RandomReader {
    object: Object,
}

impl RandomReader {
    fn new(object: Object) -> Self {
        Self { object }
    }
}

impl RandomRead for RandomReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut [u8],
        pos: usize,
    ) -> Poll<io::Result<usize>> {
        let len = if pos < self.object.len() {
            let end = std::cmp::min(self.object.len(), pos + buf.len());
            let src = &self.object[pos..end];
            let dst = &mut buf[0..src.len()];
            dst.copy_from_slice(src);
            src.len()
        } else {
            0
        };
        Poll::Ready(Ok(len))
    }
}

pub struct SequentialWriter {
    bucket: Bucket,
    object_name: String,
    object_data: Vec<u8>,
}

impl SequentialWriter {
    fn new(bucket: Bucket, object_name: String) -> Self {
        Self {
            bucket,
            object_name,
            object_data: Vec::new(),
        }
    }
}

impl SequentialWrite for SequentialWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.object_data).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut lock = Box::pin(self.bucket.clone().lock_owned());
        let mut bucket = ready!(Pin::new(&mut lock).poll(cx));
        let name = std::mem::take(&mut self.object_name);
        let data = std::mem::take(&mut self.object_data);
        bucket.insert(name, Arc::new(data));
        Poll::Ready(Ok(()))
    }
}
