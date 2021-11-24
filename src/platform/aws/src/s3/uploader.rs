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
use storage::{async_trait, ObjectUploader};
use tokio::task::JoinHandle;

use super::error::{Error, Result};

pub struct S3ObjectUploader {
    client: Client,
    bucket_name: String,
    key: String,
    upload_id: String,
    part_handles: Vec<JoinHandle<Result<CompletedPart>>>,
}

impl S3ObjectUploader {
    pub fn new(client: Client, bucket_name: String, key: String, upload_id: String) -> Self {
        Self {
            client,
            bucket_name,
            key,
            upload_id,
            part_handles: vec![],
        }
    }

    fn upload_part(&mut self, data: Bytes) {
        let cli = self.client.clone();
        let bucket = self.bucket_name.to_owned();
        let key = self.key.to_owned();
        let upload_id = self.upload_id.to_owned();
        let part_number = (self.part_handles.len() + 1) as i32;

        let part_handle = tokio::task::spawn(async move {
            let output = cli
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data))
                .send()
                .await?;
            Ok(CompletedPart::builder()
                .e_tag(output.e_tag.unwrap())
                .part_number(part_number)
                .build())
        });
        self.part_handles.push(part_handle);
    }

    async fn collect_parts(mut self) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        for handle in self.part_handles.split_off(0) {
            let part = handle.await??;
            parts.push(part);
        }
        Ok(parts)
    }
}

const UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl ObjectUploader for S3ObjectUploader {
    type Error = Error;

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let mut data = Bytes::copy_from_slice(buf);
        if data.len() < UPLOAD_PART_SIZE * 2 {
            self.upload_part(data);
            return Ok(());
        }

        while data.len() >= UPLOAD_PART_SIZE * 2 {
            let batch = data.split_to(UPLOAD_PART_SIZE);
            self.upload_part(batch);
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
            .await?;

        let output = client
            .head_object()
            .bucket(bucket_name)
            .key(key)
            .send()
            .await?;

        Ok(output.content_length as usize)
    }
}
