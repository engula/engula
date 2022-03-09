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

use crate::{
    fs::{FileBucket, SequentialWriter},
    proto::*,
    Result,
};

#[derive(Clone)]
pub struct Bucket {
    inner: Arc<BucketInner>,
}

impl Bucket {
    pub fn new(
        name: String,
        tenant: String,
        options: BucketOptions,
        file_bucket: FileBucket,
    ) -> Self {
        let inner = BucketInner::new(name, tenant, options, file_bucket);
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn tenant(&self) -> &str {
        &self.inner.tenant
    }

    pub async fn desc(&self) -> BucketDesc {
        self.inner.desc().await
    }

    pub async fn new_sequential_writer(&self, name: &str) -> Result<SequentialWriter> {
        self.inner.file_bucket.new_sequential_writer(name).await
    }
}

struct BucketInner {
    name: String,
    tenant: String,
    options: BucketOptions,
    file_bucket: FileBucket,
}

impl BucketInner {
    fn new(name: String, tenant: String, options: BucketOptions, file_bucket: FileBucket) -> Self {
        Self {
            name,
            tenant,
            options,
            file_bucket,
        }
    }

    async fn desc(&self) -> BucketDesc {
        BucketDesc {
            name: self.name.clone(),
            tenant: self.tenant.clone(),
            options: Some(self.options.clone()),
            properties: None,
        }
    }
}
