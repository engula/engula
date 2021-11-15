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
    object::RemoteObject,
    proto::{DeleteObjectRequest, UploadObjectRequest},
};
use crate::{async_trait, Bucket, ObjectUploader};

pub struct RemoteBucket {
    client: Client,
    bucket: String,
}

impl RemoteBucket {
    pub fn new(client: Client, bucket: String) -> RemoteBucket {
        RemoteBucket { client, bucket }
    }
}

#[async_trait]
impl Bucket<RemoteObject> for RemoteBucket {
    type ObjectUploader = RemoteObjectUploader;

    async fn object(&self, name: &str) -> Result<RemoteObject> {
        let object = RemoteObject::new(self.client.clone(), self.bucket.clone(), name.to_owned());
        Ok(object)
    }

    async fn upload_object(&self, name: &str) -> Result<Self::ObjectUploader> {
        Ok(RemoteObjectUploader::new(
            self.client.clone(),
            self.bucket.clone(),
            name.to_owned(),
        ))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        let input = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            object: name.to_owned(),
        };
        let _ = self.client.delete_object(input).await?;
        Ok(())
    }
}

pub struct RemoteObjectUploader {
    client: Client,
    bucket: String,
    object: String,
    content: Vec<u8>,
}

impl RemoteObjectUploader {
    fn new(client: Client, bucket: String, object: String) -> RemoteObjectUploader {
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

    async fn finish(mut self) -> Result<usize> {
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
