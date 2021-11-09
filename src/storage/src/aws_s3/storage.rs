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
    model::{BucketLocationConstraint, CreateBucketConfiguration, PublicAccessBlockConfiguration},
    Client, Config, Credentials, Region,
};

use crate::{
    aws_s3::bucket_handle::S3BucketHandle, bucket_handle::BucketHandle, error::StorageResult,
    ObjectStorage, StorageError,
};

pub struct RemoteS3Storage {
    client: Client,
}

impl RemoteS3Storage {
    pub fn new(
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        let credentials = Credentials::from_keys(access_key, secret_key, None);
        let client = Client::from_conf(
            Config::builder()
                .region(Some(Region::new(region.into())))
                .credentials_provider(credentials)
                .build(),
        );
        Self { client }
    }
}

#[async_trait]
impl ObjectStorage for RemoteS3Storage {
    fn bucket(&self, name: impl Into<String>) -> Box<dyn BucketHandle> {
        let handle = S3BucketHandle::new(self.client.clone(), name.into());
        Box::new(handle)
    }

    async fn create_bucket(&self, name: &str, location: &str) -> StorageResult<()> {
        self.client
            .create_bucket()
            .bucket(name)
            .create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from(location))
                    .build(),
            )
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })?;

        self.client
            .put_public_access_block()
            .bucket(name)
            .public_access_block_configuration(
                PublicAccessBlockConfiguration::builder()
                    .restrict_public_buckets(true)
                    .block_public_policy(true)
                    .ignore_public_acls(true)
                    .block_public_acls(true)
                    .build(),
            )
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })
    }

    async fn list_buckets(&self) -> StorageResult<Vec<String>> {
        self.client
            .list_buckets()
            .send()
            .await
            .map(|bs| {
                bs.buckets
                    .unwrap_or(vec![])
                    .iter()
                    .map(|b| b.name.clone().unwrap_or_default())
                    .collect()
            })
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })
    }

    async fn delete_bucket(&self, name: &str) -> StorageResult<()> {
        self.client
            .delete_bucket()
            .bucket(name)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let msg = e.to_string();
                StorageError { msg }
            })
    }
}
