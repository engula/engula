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
    model::{BucketCannedAcl, BucketLocationConstraint, CreateBucketConfiguration},
    Client, Config,
};
use storage::{async_trait, Storage};

use super::{error::Result, object::S3Object, uploader::S3ObjectUploader};

pub struct S3Storage {
    client: Client,
    region: String,
}

impl S3Storage {
    pub fn new(region: impl Into<String>, config: Config) -> Self {
        Self {
            client: Client::from_conf(config),
            region: region.into(),
        }
    }
}

#[async_trait]
impl Storage<S3Object> for S3Storage {
    type ObjectUploader = S3ObjectUploader;

    async fn create_bucket(&self, name: &str) -> Result<()> {
        let region: &str = &self.region;
        let location = BucketLocationConstraint::from(region);
        let config = CreateBucketConfiguration::builder()
            .location_constraint(location)
            .build();
        self.client
            .create_bucket()
            .acl(BucketCannedAcl::Private)
            .bucket(name.to_owned())
            .create_bucket_configuration(config)
            .send()
            .await?;

        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.client
            .delete_bucket()
            .bucket(name.to_owned())
            .send()
            .await?;
        Ok(())
    }

    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<S3Object> {
        Ok(S3Object::new(
            self.client.clone(),
            bucket_name.to_owned(),
            object_name.to_owned(),
        ))
    }

    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<S3ObjectUploader> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await?;

        let upload_id = output.upload_id.unwrap();
        Ok(S3ObjectUploader::new(
            self.client.clone(),
            bucket_name.to_owned(),
            object_name.to_owned(),
            upload_id,
        ))
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await?;
        Ok(())
    }
}
