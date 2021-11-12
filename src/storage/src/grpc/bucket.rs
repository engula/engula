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

use futures::{future, stream};

use super::{
    client::Client,
    object::RemoteObject,
    proto::{DeleteObjectRequest, ListObjectsRequest, UploadObjectRequest},
};
use crate::{async_trait, Bucket, Object, ObjectUploader, Result, ResultStream};

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
impl Bucket for RemoteBucket {
    async fn object(&self, name: &str) -> Result<Box<dyn Object>> {
        let object = RemoteObject::new(self.client.clone(), self.bucket.clone(), name.to_owned());
        Ok(Box::new(object))
    }

    async fn list_objects(&self) -> ResultStream<String> {
        let input = ListObjectsRequest {
            bucket: self.bucket.clone(),
        };
        match self.client.list_objects(input).await {
            Ok(output) => {
                let object_names = output
                    .objects
                    .into_iter()
                    .map(Ok)
                    .collect::<Vec<Result<String>>>();
                Box::new(stream::iter(object_names))
            }
            Err(err) => Box::new(stream::once(future::err(err.into()))),
        }
    }

    async fn upload_object(&self, name: &str) -> Box<dyn ObjectUploader> {
        let uploader =
            RemoteObjectUploader::new(self.client.clone(), self.bucket.clone(), name.to_owned());
        Box::new(uploader)
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

struct RemoteObjectUploader {
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
    async fn write(&mut self, buf: &[u8]) {
        self.content.extend_from_slice(buf);
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
