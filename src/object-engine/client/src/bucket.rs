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

use object_engine_filestore::SequentialWrite;
use object_engine_master::proto::*;

use crate::{env::Iter, BucketEnv, Env, Error, Result, TenantEnv};

#[derive(Clone)]
pub struct Bucket<E: Env> {
    env: E,
    bucket: <<E as Env>::TenantEnv as TenantEnv>::BucketEnv,
}

impl<E: Env> Bucket<E> {
    pub(crate) fn new(env: E, bucket: <<E as Env>::TenantEnv as TenantEnv>::BucketEnv) -> Self {
        Self { env, bucket }
    }

    pub fn name(&self) -> &str {
        self.bucket.name()
    }

    pub fn tenant(&self) -> &str {
        self.bucket.tenant()
    }

    pub async fn desc(&self) -> Result<BucketDesc> {
        let req = DescribeBucketRequest {
            tenant: self.tenant().to_owned(),
            bucket: self.name().to_owned(),
        };
        let req = request_union::Request::DescribeBucket(req);
        let res = self.env.handle_union(req).await?;
        let desc = if let response_union::Response::DescribeBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing bucket descriptor"))
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.bucket.get(key).await
    }

    pub async fn iter(&self) -> Result<Box<dyn Iter>> {
        self.bucket.iter().await
    }

    pub(crate) async fn new_sequential_writer(
        &self,
        name: &str,
    ) -> Result<Box<dyn SequentialWrite>> {
        self.bucket.new_sequential_writer(name).await
    }
}
