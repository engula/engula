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

use async_trait::async_trait;
use aws_sdk_s3::{
    model::{Delete, ObjectIdentifier},
    ByteStream, Client,
};
use bytes::Bytes;

use crate::{
    aws_s3::object_handle::{S3ObjectReader, S3ObjectWriter},
    bucket_handle::BucketHandle,
    error::StorageResult,
    object_handle::{ObjectReader, ObjectWriter},
    StorageError,
};

pub struct S3BucketHandle {
    client: Client,
    bucket_name: String,
}

impl S3BucketHandle {
    pub fn new(client: Client, bucket_name: String) -> Self {
        S3BucketHandle {
            client,
            bucket_name,
        }
    }
}

#[async_trait]
impl BucketHandle for S3BucketHandle {
    async fn new_writer(&self, name: &str) -> StorageResult<Box<dyn ObjectWriter>> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket_name.clone())
            .key(name.to_string())
            .send()
            .await
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })?;

        let upload_id = output.upload_id.unwrap();
        Ok(Box::new(S3ObjectWriter::new(
            self.client.clone(),
            self.bucket_name.clone(),
            name.to_string(),
            upload_id,
        )))
    }

    async fn new_reader(&self, name: &str) -> Box<dyn ObjectReader> {
        Box::new(S3ObjectReader::new(
            self.client.clone(),
            self.bucket_name.clone(),
            name.to_string(),
        ))
    }

    async fn delete_object(&self, key: &str) -> StorageResult<()> {
        self.client
            .delete_object()
            .bucket(self.bucket_name.clone())
            .key(key.to_string())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })
    }

    async fn delete_objects(&self, key: Vec<String>) -> StorageResult<()> {
        let objs: Vec<ObjectIdentifier> = key
            .iter()
            .map(|k| ObjectIdentifier::builder().key(k).build())
            .collect();
        self.client
            .delete_objects()
            .delete(Delete::builder().set_objects(Some(objs)).build())
            .bucket(self.bucket_name.clone())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })
    }

    async fn put_object(&self, name: &str, body: Bytes) -> StorageResult<()> {
        self.client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(name.to_string())
            .body(ByteStream::from(body))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })
    }
}
