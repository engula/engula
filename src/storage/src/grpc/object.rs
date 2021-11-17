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
    proto::ReadObjectRequest,
};
use crate::{async_trait, Object};

pub struct RemoteObject {
    client: Client,
    bucket: String,
    object: String,
}

impl RemoteObject {
    pub fn new(client: Client, bucket: String, object: String) -> RemoteObject {
        RemoteObject {
            client,
            bucket,
            object,
        }
    }
}

#[async_trait]
impl Object for RemoteObject {
    type Error = Error;

    async fn read_at(&self, buf: &mut [u8], offset: usize) -> Result<usize> {
        let input = ReadObjectRequest {
            bucket: self.bucket.clone(),
            object: self.object.clone(),
            offset: offset as i64,
            length: buf.len() as i64,
        };
        let output = self.client.read_object(input).await?;
        let content = output.content;
        // TODO: refine the interface to avoid copy here.
        assert!(content.len() <= buf.len());
        buf[..content.len()].copy_from_slice(&content);
        Ok(content.len())
    }
}
