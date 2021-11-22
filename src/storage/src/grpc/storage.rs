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
    error::Result,
    object::RemoteObject,
    proto::{CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest},
    RemoteObjectUploader,
};
use crate::{async_trait, Storage};

pub struct RemoteStorage {
    client: Client,
}

impl RemoteStorage {
    pub async fn connect(addr: &str) -> Result<RemoteStorage> {
        let client = Client::connect(addr).await?;
        Ok(RemoteStorage { client })
    }
}

#[async_trait]
impl Storage<RemoteObject> for RemoteStorage {
    type ObjectUploader = RemoteObjectUploader;

    async fn create_bucket(&self, name: &str) -> Result<()> {
        let input = CreateBucketRequest {
            bucket: name.to_owned(),
        };
        self.client.create_bucket(input).await?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let input = DeleteBucketRequest {
            bucket: name.to_owned(),
        };
        self.client.delete_bucket(input).await?;
        Ok(())
    }

    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<RemoteObject> {
        let object = RemoteObject::new(
            self.client.clone(),
            bucket_name.to_owned(),
            object_name.to_owned(),
        );
        Ok(object)
    }

    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::ObjectUploader> {
        Ok(RemoteObjectUploader::new(
            self.client.clone(),
            bucket_name.to_owned(),
            object_name.to_owned(),
        ))
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        let input = DeleteObjectRequest {
            bucket: bucket_name.to_owned(),
            object: object_name.to_owned(),
        };
        let _ = self.client.delete_object(input).await?;
        Ok(())
    }
}
