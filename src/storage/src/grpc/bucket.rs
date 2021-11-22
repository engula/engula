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

use super::{
    client::Client,
    error::{Error, Result},
    proto::UploadObjectRequest,
};
use crate::{async_trait, ObjectUploader};

pub struct RemoteObjectUploader {
    client: Client,
    bucket: String,
    object: String,
    content: Vec<u8>,
}

impl RemoteObjectUploader {
    pub fn new(client: Client, bucket: String, object: String) -> RemoteObjectUploader {
        RemoteObjectUploader {
            client,
            bucket,
            object,
            content: Vec::new(),
        }
    }
}

#[async_trait]
impl ObjectUploader for RemoteObjectUploader {
    type Error = Error;

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.content.extend_from_slice(buf);
        Ok(())
    }

    async fn finish(self) -> Result<usize> {
        let len = self.content.len();
        let input = UploadObjectRequest {
            bucket: self.bucket,
            object: self.object,
            content: self.content,
        };
        let _ = self.client.upload_object(input).await?;
        Ok(len)
    }
}
