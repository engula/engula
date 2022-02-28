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

use std::sync::Arc;

use engula_apis::*;
use object_engine_master::proto::*;

use crate::{master::Master, BucketTxn, Error, Result};

#[derive(Clone)]
pub struct Bucket {
    inner: Arc<BucketInner>,
}

impl Bucket {
    pub(crate) fn new(bucket: String, tenant: String, master: Master) -> Self {
        let inner = BucketInner {
            tenant,
            bucket,
            master,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn desc(&self) -> Result<BucketDesc> {
        let req = LookupBucketRequest {
            name: self.inner.bucket.clone(),
            ..Default::default()
        };
        let req = bucket_request_union::Request::LookupBucket(req);
        let res = self.inner.bucket_union_call(req).await?;
        let desc = if let bucket_response_union::Response::LookupBucket(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or(Error::InvalidResponse)
    }

    pub async fn eval(&self, _expr: Expr) -> Result<ExprResult> {
        todo!();
    }

    pub async fn metadata(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!();
    }

    pub fn begin(&self) -> BucketTxn {
        todo!();
    }
}

struct BucketInner {
    tenant: String,
    bucket: String,
    master: Master,
}

impl BucketInner {
    async fn bucket_union_call(
        &self,
        req: bucket_request_union::Request,
    ) -> Result<bucket_response_union::Response> {
        self.master.bucket_union(self.tenant.clone(), req).await
    }
}
