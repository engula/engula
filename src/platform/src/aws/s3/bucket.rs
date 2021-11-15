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
use storage::{async_trait, Bucket, ObjectUploader};
use tokio::task::JoinHandle;

use super::{
    error::{to_storage_err, Error, Result},
    object::S3Object,
};

pub struct S3Bucket {
    client: Client,
    bucket_name: String,
}

impl S3Bucket {
    pub fn new(client: Client, bucket_name: impl Into<String>) -> Self {
        Self {
            client,
            bucket_name: bucket_name.into(),
        }
    }
}

#[async_trait]
impl Bucket<S3Object> for S3Bucket {
    type ObjectUploader = S3UploadObject;

    async fn object(&self, name: &str) -> Result<S3Object> {
        Ok(S3Object::new(
            self.client.clone(),
            self.bucket_name.to_owned(),
            name.to_string(),
        ))
    }

    async fn upload_object(&self, name: &str) -> Result<S3UploadObject> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket_name.to_owned())
            .key(name.to_string())
            .send()
            .await
            .map_err(to_storage_err)?;

        let upload_id = output.upload_id.unwrap();
        Ok(S3UploadObject::new(
            self.client.clone(),
            self.bucket_name.to_owned(),
            name.to_string(),
            upload_id,
        ))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(self.bucket_name.to_owned())
            .key(name.to_owned())
            .send()
            .await
            .map(|_| ())
            .map_err(to_storage_err)
    }
}

pub struct S3UploadObject {
    client: Client,
    bucket_name: String,
    key: String,
    upload_id: String,
    part_handles: Box<Vec<JoinHandle<Result<CompletedPart>>>>,
}

impl S3UploadObject {
    pub fn new(client: Client, bucket_name: String, key: String, upload_id: String) -> Self {
        Self {
            client,
            bucket_name,
            key,
            upload_id,
            part_handles: Box::new(vec![]),
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
                .map_err(to_storage_err);
            r
        });
        self.part_handles.push(part_handle);
    }

    async fn collect_parts(mut self) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        for handle in self.part_handles.split_off(0) {
            match handle.await {
                Ok(rpart) => match rpart {
                    Ok(part) => parts.push(part),
                    Err(e) => return Err(e),
                },
                Err(_e) => return Err(Error::WaitUploadTaskDoneError),
            }
        }
        Ok(parts)
    }
}

const UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl ObjectUploader for S3UploadObject {
    type Error = Error;

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let data = Bytes::copy_from_slice(buf);
        if data.len() < UPLOAD_PART_SIZE * 2 {
            self.upload_part(data);
            return Ok(());
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
        Ok(())
    }

    async fn finish(mut self) -> Result<usize> {
        let bucket_name = self.bucket_name.to_owned();
        let key = self.key.to_owned();
        let client = self.client.clone();
        let upload_id = self.upload_id.to_owned();
        let parts = self.collect_parts().await?;
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        client
            .clone()
            .complete_multipart_upload()
            .bucket(bucket_name.to_owned())
            .key(key.to_owned())
            .upload_id(upload_id.to_owned())
            .multipart_upload(upload)
            .send()
            .await
            .map(|_| ())
            .map_err(to_storage_err)?;

        client
            .head_object()
            .bucket(bucket_name)
            .key(key)
            .send()
            .await
            .map(|output| output.content_length as usize)
            .map_err(to_storage_err)
    }
}
