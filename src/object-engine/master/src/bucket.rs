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

use tokio::sync::Mutex;

use crate::{
    fs::{FileBucket, SequentialWriter},
    proto::*,
    Result,
};

#[derive(Clone)]
pub struct Bucket {
    inner: Arc<Mutex<BucketInner>>,
}

impl Bucket {
    pub fn new(desc: BucketDesc, file_bucket: FileBucket) -> Self {
        let inner = BucketInner::new(desc, file_bucket);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> BucketDesc {
        let inner = self.inner.lock().await;
        inner.desc.clone()
    }

    pub async fn new_sequential_writer(&self, name: &str) -> Result<SequentialWriter> {
        let inner = self.inner.lock().await;
        inner.file_bucket.new_sequential_writer(name).await
    }
}

struct BucketInner {
    desc: BucketDesc,
    file_bucket: FileBucket,
}

impl BucketInner {
    fn new(desc: BucketDesc, file_bucket: FileBucket) -> Self {
        Self { desc, file_bucket }
    }
}
