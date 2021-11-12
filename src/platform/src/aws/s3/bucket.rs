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

use aws_sdk_s3::{
    model::{CompletedMultipartUpload, CompletedPart},
    ByteStream, Client,
};
use bytes::Bytes;
use futures::{future, stream};
use storage::{async_trait, Bucket, Error, Object, ObjectUploader, Result, ResultStream};
use tokio::task::JoinHandle;

use super::object::S3Object;

pub(crate) struct S3Bucket {
    client: Client,
    bucket_name: String,
}

impl S3Bucket {
    pub(crate) fn new(client: Client, bucket_name: impl Into<String>) -> Self {
        Self {
            client,
            bucket_name: bucket_name.into(),
        }
    }
}

#[async_trait]
impl Bucket for S3Bucket {
    async fn object(&self, name: &str) -> Result<Box<dyn Object>> {
        Ok(Box::new(S3Object::new(
            self.client.clone(),
            self.bucket_name.to_owned(),
            name.to_string(),
        )))
    }

    async fn list_objects(&self) -> ResultStream<String> {
        let result = self
            .client
            .list_objects()
            .bucket(self.bucket_name.to_owned())
            .send()
            .await;
        match result {
            Ok(output) => {
                let objects = output
                    .contents
                    .unwrap_or(vec![])
                    .into_iter()
                    .filter_map(|content| content.key.to_owned())
                    .map(Ok);
                Box::new(stream::iter(objects))
            }
            Err(e) => Box::new(stream::once(future::err(Error::AwsSDK(format!(
                "list object fail: {}",
                e.to_string()
            ))))),
        }
    }

    async fn upload_object(&self, name: &str) -> Result<Box<dyn ObjectUploader>> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket_name.to_owned())
            .key(name.to_string())
            .send()
            .await
            .map_err(|e| {
                Error::AwsSDK(format!(
                    "create upload object fail, bucket: '{}', key: '{}', {}",
                    self.bucket_name.to_owned(),
                    name.to_string(),
                    e.to_string()
                ))
            })?;

        let upload_id = output.upload_id.unwrap();
        Ok(Box::new(S3UploadObject::new(
            self.client.clone(),
            self.bucket_name.to_owned(),
            name.to_string(),
            upload_id,
        )))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(self.bucket_name.to_owned())
            .key(name.to_owned())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                Error::AwsSDK(format!(
                    "delete object '{}' fail, {}",
                    name.to_owned(),
                    e.to_string()
                ))
            })
    }
}

pub(crate) struct S3UploadObject {
    client: Client,
    bucket_name: String,
    key: String,
    upload_id: String,
    part_handles: Vec<JoinHandle<Result<CompletedPart>>>,
}

impl S3UploadObject {
    pub(crate) fn new(client: Client, bucket_name: String, key: String, upload_id: String) -> Self {
        Self {
            client,
            bucket_name,
            key,
            upload_id,
            part_handles: vec![],
        }
    }

    fn upload_part(&mut self, data: Bytes) {
        let f_cli = self.client.clone();
        let f_bucket = self.bucket_name.to_owned();
        let f_test_key = self.key.to_owned();
        let f_upload_id = self.upload_id.to_owned();
        let part_number = (self.part_handles.len() + 1) as i32;

        let part_handle = tokio::task::spawn(async move {
            let r: Result<CompletedPart> = f_cli
                .upload_part()
                .bucket(f_bucket)
                .key(f_test_key)
                .upload_id(f_upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data))
                .send()
                .await
                .map(|output| {
                    CompletedPart::builder()
                        .e_tag(output.e_tag.unwrap())
                        .part_number(part_number)
                        .build()
                })
                .map_err(|e| Error::AwsSDK(format!("write upload part fail, {}", e.to_string())));
            r
        });
        self.part_handles.push(part_handle);
    }

    async fn collect_parts(&mut self) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        for handle in self.part_handles.split_off(0) {
            match handle.await {
                Ok(rpart) => match rpart {
                    Ok(part) => parts.push(part),
                    Err(e) => return Err(e),
                },
                Err(e) => {
                    return Err(Error::AwsSDK(format!(
                        "wait uploading parts finish fail, {}",
                        e.to_string()
                    )))
                }
            }
        }
        Ok(parts)
    }
}

const UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl ObjectUploader for S3UploadObject {
    async fn write(&mut self, buf: &[u8]) {
        let data = Bytes::copy_from_slice(buf);
        if data.len() < UPLOAD_PART_SIZE * 2 {
            self.upload_part(data);
            return;
        }

        let mut offset = 0;
        while offset < data.len() {
            let end = if data.len() - offset < UPLOAD_PART_SIZE * 2 {
                data.len()
            } else {
                offset + UPLOAD_PART_SIZE
            };
            self.upload_part(data.slice(offset..end));
            offset = end;
        }
        self.upload_part(data);
    }

    async fn finish(&mut self) -> Result<usize> {
        let parts = self.collect_parts().await?;
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(self.bucket_name.to_owned())
            .key(self.key.to_owned())
            .upload_id(self.upload_id.to_owned())
            .multipart_upload(upload)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| Error::AwsSDK(format!("complete upload fail, {}", e.to_string())))?;

        self.client
            .head_object()
            .bucket(self.bucket_name.to_owned())
            .key(self.key.to_owned())
            .send()
            .await
            .map(|output| output.content_length as usize)
            .map_err(|e| {
                Error::AwsSDK(format!(
                    "get object size for '{}' fail, {}",
                    self.key.to_owned(),
                    e.to_string(),
                ))
            })
    }
}
