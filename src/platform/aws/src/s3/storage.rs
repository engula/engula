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
    model::{
        BucketLocationConstraint, CreateBucketConfiguration,
        PublicAccessBlockConfiguration,
    },
    Client, Config, Credentials, Region,
};
use storage::{async_trait, Storage};

use super::{bucket::S3Bucket, error::Result, object::S3Object};

pub struct S3Storage {
    client: Client,
    region: String,
}

impl S3Storage {
    pub fn new(
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        let credentials = Credentials::from_keys(access_key, secret_key, None);
        let region = region.into();
        let client = Client::from_conf(
            Config::builder()
                .region(Some(Region::new(region.to_owned())))
                .credentials_provider(credentials)
                .build(),
        );
        Self::from_client(client, region)
    }

    pub fn from_client(client: Client, region: impl Into<String>) -> Self {
        let region = region.into();
        Self { client, region }
    }
}

#[async_trait]
impl Storage<S3Object, S3Bucket> for S3Storage {
    async fn bucket(&self, name: &str) -> Result<S3Bucket> {
        self.client
            .head_bucket()
            .bucket(name.to_owned())
            .send()
            .await?;

        Ok(S3Bucket::new(self.client.clone(), name))
    }

    async fn create_bucket(&self, name: &str) -> Result<S3Bucket> {
        let region: &str = &self.region;
        let location = BucketLocationConstraint::from(region);
        let config = CreateBucketConfiguration::builder()
            .location_constraint(location)
            .build();
        self.client
            .create_bucket()
            .bucket(name.to_owned())
            .create_bucket_configuration(config)
            .send()
            .await?;

        self.client
            .put_public_access_block()
            .bucket(name.to_owned())
            .public_access_block_configuration(
                PublicAccessBlockConfiguration::builder()
                    .restrict_public_buckets(true)
                    .block_public_policy(true)
                    .ignore_public_acls(true)
                    .block_public_acls(true)
                    .build(),
            )
            .send()
            .await?;

        Ok(S3Bucket::new(self.client.clone(), name))
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.client
            .delete_bucket()
            .bucket(name.to_owned())
            .send()
            .await?;
        Ok(())
    }
}
