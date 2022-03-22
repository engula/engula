// Copyright 2022 The Engula Authors.
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

use std::path::PathBuf;

use object_engine_filestore::SequentialWrite;
use object_engine_lsmstore::{Key, MergingIterator, ValueType};
use object_engine_master::{proto::*, Bucket, Master, Tenant};

use super::Iter;
use crate::{async_trait, Result};

#[derive(Clone)]
pub struct Env {
    master: Master,
}

impl Env {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let master = Master::open(path).await?;
        Ok(Self { master })
    }
}

#[async_trait]
impl super::Env for Env {
    type TenantEnv = TenantEnv;

    async fn tenant(&self, name: &str) -> Result<Self::TenantEnv> {
        let tenant = self.master.tenant(name).await?;
        Ok(TenantEnv { tenant })
    }

    async fn handle_batch(&self, req: BatchRequest) -> Result<BatchResponse> {
        self.master.handle_batch(req).await
    }
}

#[derive(Clone)]
pub struct TenantEnv {
    tenant: Tenant,
}

#[async_trait]
impl super::TenantEnv for TenantEnv {
    type BucketEnv = BucketEnv;

    fn name(&self) -> &str {
        self.tenant.name()
    }

    async fn bucket(&self, name: &str) -> Result<Self::BucketEnv> {
        let bucket = self.tenant.bucket(name).await?;
        Ok(BucketEnv { bucket })
    }
}

#[derive(Clone)]
pub struct BucketEnv {
    bucket: Bucket,
}

#[async_trait]
impl super::BucketEnv for BucketEnv {
    fn name(&self) -> &str {
        self.bucket.name()
    }

    fn tenant(&self) -> &str {
        self.bucket.tenant()
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn SequentialWrite>> {
        self.bucket.new_sequential_writer(name).await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.bucket.get(key).await
    }

    async fn iter(&self) -> Result<Box<dyn Iter>> {
        Ok(Box::new(LocalIter {
            iter: self.bucket.iter().await?,
            last_id: None,
        }))
    }
}

struct LocalIter {
    iter: MergingIterator,
    last_id: Option<Vec<u8>>,
}

#[async_trait]
impl Iter for LocalIter {
    fn key(&self) -> Vec<u8> {
        self.iter.key().id().to_owned()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }

    async fn seek_to_first(&mut self) -> Result<()> {
        self.iter.seek_to_first().await?;
        self.skip_invisible_if_needed().await?;
        Ok(())
    }

    async fn seek(&mut self, target: &[u8]) -> Result<()> {
        let key = Key::encode_to_vec(target, u64::MAX, object_engine_lsmstore::ValueType::Put);
        self.iter.seek(key.as_slice().into()).await?;
        self.skip_invisible_if_needed().await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<()> {
        loop {
            self.iter.next().await?;
            if !self.iter.valid() {
                break;
            }
            if let ValueType::Delete = self.iter.key().tp() {
                self.last_id = Some(self.iter.key().id().to_owned());
                continue;
            }
            if self.last_id.is_none() {
                self.last_id = Some(self.iter.key().id().to_owned());
                break;
            }
            if self.last_id.as_ref().unwrap() != self.iter.key().id() {
                self.last_id = Some(self.iter.key().id().to_owned());
                break;
            }
            // TODO: stats & ratelimit ?
        }
        Ok(())
    }
}

impl LocalIter {
    async fn skip_invisible_if_needed(&mut self) -> Result<()> {
        if !self.iter.valid() {
            return Ok(());
        }
        self.last_id = Some(self.iter.key().id().to_owned());
        match self.iter.key().tp() {
            ValueType::Delete => {
                self.next().await?;
            }
            ValueType::Merge => todo!(),
            _ => {}
        }
        Ok(())
    }
}
