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

use engula_futures::stream::BatchResultStreamExt;

use super::{random_reader::RandomReader, Orchestrator};
use crate::{async_trait, Result, Storage};

pub struct CachedStorage<B, O>
where
    B: Storage,
    O: Orchestrator,
{
    base: B,
    #[allow(dead_code)]
    orch: O,
    caches: Vec<O::Instance>,
}

const CACHE_BUCKET_NAME: &str = "default";

fn cache_object_name(bucket: &str, object: &str) -> String {
    format!("{}.{}", bucket, object)
}

impl<B, O> CachedStorage<B, O>
where
    B: Storage,
    O: Orchestrator,
{
    pub async fn open(base: B, orch: O) -> Result<Self> {
        let mut list = orch.list_instances().await?;
        let mut caches = list.collect(10).await?;
        for cache in &mut caches {
            cache.create_bucket(CACHE_BUCKET_NAME).await?;
        }
        Ok(Self { base, orch, caches })
    }
}

#[async_trait]
impl<B, O> Storage for CachedStorage<B, O>
where
    B: Storage,
    O: Orchestrator,
{
    type BucketLister = B::BucketLister;
    type ObjectLister = B::ObjectLister;
    type RandomReader =
        RandomReader<B::RandomReader, <<O as Orchestrator>::Instance as Storage>::RandomReader>;
    type SequentialWriter = B::SequentialWriter;

    async fn list_buckets(&self) -> Result<Self::BucketLister> {
        self.base.list_buckets().await
    }

    async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        self.base.create_bucket(bucket_name).await
    }

    async fn delete_bucket(&self, bucket_name: &str) -> Result<()> {
        self.base.delete_bucket(bucket_name).await
    }

    async fn list_objects(&self, bucket_name: &str) -> Result<Self::ObjectLister> {
        self.base.list_objects(bucket_name).await
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        let cache_object_name = cache_object_name(bucket_name, object_name);
        for cache in &self.caches {
            cache
                .delete_object(CACHE_BUCKET_NAME, &cache_object_name)
                .await?;
        }
        self.base.delete_object(bucket_name, object_name).await
    }

    async fn new_random_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader> {
        let base_reader = self
            .base
            .new_random_reader(bucket_name, object_name)
            .await?;
        let mut cache_reader = None;
        let cache_object_name = cache_object_name(bucket_name, object_name);
        for cache in &self.caches {
            if let Ok(reader) = cache
                .new_random_reader(CACHE_BUCKET_NAME, &cache_object_name)
                .await
            {
                cache_reader = Some(reader);
            }
        }
        Ok(RandomReader::new(base_reader, cache_reader))
    }

    async fn new_sequential_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialWriter> {
        self.base
            .new_sequential_writer(bucket_name, object_name)
            .await
    }
}
