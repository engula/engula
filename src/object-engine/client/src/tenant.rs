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

use object_engine_master::proto::*;

use crate::{Bucket, BulkLoad, Env, Error, Result, TenantEnv};

#[derive(Clone)]
pub struct Tenant<E: Env> {
    env: E,
    tenant: E::TenantEnv,
}

impl<E: Env> Tenant<E> {
    pub(crate) fn new(env: E, tenant: E::TenantEnv) -> Self {
        Self { env, tenant }
    }

    pub fn name(&self) -> &str {
        self.tenant.name()
    }

    pub async fn bucket(&self, name: &str) -> Result<Bucket<E>> {
        let bucket = self.tenant.bucket(name).await?;
        Ok(Bucket::new(self.env.clone(), bucket))
    }

    pub async fn create_bucket(&self, name: &str) -> Result<BucketDesc> {
        let desc = BucketDesc {
            name: name.to_owned(),
            ..Default::default()
        };
        let req = CreateBucketRequest { desc: Some(desc) };
        let req = bucket_request_union::Request::CreateBucket(req);
        let res = self
            .env
            .handle_bucket_union(self.name().to_owned(), req)
            .await?;
        let desc = if let bucket_response_union::Response::CreateBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing bucket descriptor"))
    }

    pub async fn describe_bucket(&self, name: &str) -> Result<BucketDesc> {
        let req = DescribeBucketRequest {
            name: name.to_owned(),
        };
        let req = bucket_request_union::Request::DescribeBucket(req);
        let res = self
            .env
            .handle_bucket_union(self.name().to_owned(), req)
            .await?;
        let desc = if let bucket_response_union::Response::DescribeBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing bucket descriptor"))
    }

    pub async fn begin_bulkload(&self) -> Result<BulkLoad<E>> {
        let req = BeginBulkLoadRequest {};
        let req = engine_request_union::Request::BeginBulkload(req);
        let res = self
            .env
            .handle_engine_union(self.name().to_owned(), req)
            .await?;
        if let engine_response_union::Response::BeginBulkload(res) = res {
            Ok(BulkLoad::new(
                self.env.clone(),
                res.token,
                self.tenant.clone(),
            ))
        } else {
            Err(Error::internal("missing begin bulkload response"))
        }
    }
}
