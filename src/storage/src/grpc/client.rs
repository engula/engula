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

use tonic::{transport::Channel, IntoStreamingRequest, Streaming};

use super::proto::*;
use crate::Result;

type StorageClient = storage_client::StorageClient<Channel>;

#[derive(Clone)]
pub struct Client {
    client: StorageClient,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Client> {
        let client = StorageClient::connect(addr.to_owned()).await?;
        Ok(Client { client })
    }

    pub async fn create_bucket(&self, input: CreateBucketRequest) -> Result<CreateBucketResponse> {
        let mut client = self.client.clone();
        let response = client.create_bucket(input).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_bucket(&self, input: DeleteBucketRequest) -> Result<DeleteBucketResponse> {
        let mut client = self.client.clone();
        let response = client.delete_bucket(input).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_object(&self, input: DeleteObjectRequest) -> Result<DeleteObjectResponse> {
        let mut client = self.client.clone();
        let response = client.delete_object(input).await?;
        Ok(response.into_inner())
    }

    pub async fn upload_object(
        &self,
        input: impl IntoStreamingRequest<Message = UploadObjectRequest>,
    ) -> Result<UploadObjectResponse> {
        let mut client = self.client.clone();
        let response = client.upload_object(input).await?;
        Ok(response.into_inner())
    }

    pub async fn read_object(
        &self,
        input: ReadObjectRequest,
    ) -> Result<Streaming<ReadObjectResponse>> {
        let mut client = self.client.clone();
        let response = client.read_object(input).await?;
        Ok(response.into_inner())
    }
}
