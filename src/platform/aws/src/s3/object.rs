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

use aws_sdk_s3::Client;
use storage::{async_trait, Object};

use super::error::{Error, Result};

pub struct S3Object {
    client: Client,
    bucket_name: String,
    key: String,
}

impl S3Object {
    pub fn new(client: Client, bucket_name: String, key: String) -> Self {
        Self {
            client,
            bucket_name,
            key,
        }
    }
}

#[async_trait]
impl Object for S3Object {
    type Error = Error;

    async fn read_at(&self, buf: &mut [u8], offset: usize) -> Result<usize> {
        let size = buf.len();
        let range = format!("bytes={}-{}", offset, offset + size - 1);
        let output = self
            .client
            .get_object()
            .bucket(self.bucket_name.to_owned())
            .key(self.key.to_owned())
            .range(range)
            .send()
            .await
            .map_err(Error::from)?;

        output.body.collect().await.map_err(Error::from)?;
        Ok(buf.len())
    }
}
