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
    model::{CompletedMultipartUpload, CompletedPart},
    ByteStream, Client,
};
use bytes::{Buf, Bytes};
use tokio::task::JoinHandle;

use crate::{
    error::StorageResult,
    object_handle::{ObjectReader, ObjectWriter},
    StorageError,
};

pub struct S3ObjectReader {
    client: Client,
    bucket_name: String,
    key: String,
}

impl S3ObjectReader {
    pub fn new(client: Client, bucket_name: String, key: String) -> Self {
        Self {
            client,
            bucket_name,
            key,
        }
    }
}

#[async_trait]
impl ObjectReader for S3ObjectReader {
    async fn read_at(&self, offset: i32, size: i32) -> StorageResult<Vec<u8>> {
        let range = format!("bytes={}-{}", offset, offset + size - 1);
        let rs = self
            .client
            .get_object()
            .bucket(self.bucket_name.clone())
            .key(self.key.clone())
            .range(range)
            .send()
            .await;

        match rs {
            Ok(output) => match output.body.collect().await {
                Ok(mut bytes) => {
                    let mut data = vec![0; bytes.remaining()];
                    bytes.copy_to_slice(&mut data);
                    Ok(data)
                }
                Err(err) => Err(StorageError {
                    msg: err.to_string(),
                }),
            },
            Err(err) => Err(StorageError {
                msg: err.to_string(),
            }),
        }
    }
}

pub struct S3ObjectWriter {
    client: Client,
    bucket_name: String,
    key: String,
    upload_id: String,
    part_handles: Vec<JoinHandle<StorageResult<CompletedPart>>>,
}

impl S3ObjectWriter {
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
        let f_cli = self.client.clone();
        let f_bucket = self.bucket_name.clone();
        let f_test_key = self.key.clone();
        let f_upload_id = self.upload_id.clone();
        let part_number = (self.part_handles.len() + 1) as i32;

        let part_handle = tokio::task::spawn(async move {
            let r: StorageResult<CompletedPart> = f_cli
                .clone()
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
                .map_err(|e| {
                    let msg = e.to_string();
                    StorageError { msg }
                });
            r
        });
        self.part_handles.push(part_handle);
    }

    async fn collect_parts(&mut self) -> StorageResult<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        for handle in self.part_handles.split_off(0) {
            match handle.await {
                Ok(rpart) => match rpart {
                    Ok(part) => parts.push(part),
                    Err(e) => return Err(e),
                },
                Err(e) => return Err(StorageError { msg: e.to_string() }),
            }
        }
        Ok(parts)
    }
}

const UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl ObjectWriter for S3ObjectWriter {
    async fn write(&mut self, data: Bytes) {
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

    async fn finish(&mut self) -> StorageResult<()> {
        let parts = self.collect_parts().await?;
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(self.bucket_name.clone())
            .key(self.key.clone())
            .upload_id(self.upload_id.clone())
            .multipart_upload(upload)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })
    }
}
