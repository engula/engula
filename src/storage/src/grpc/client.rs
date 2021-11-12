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

use tonic::{transport::Channel, Request};

use super::{error::Result, proto::*};

type StorageClient = storage_client::StorageClient<Channel>;

#[derive(Clone)]
pub struct Client {
    client: StorageClient,
}

macro_rules! method {
    ($name:ident, $input:ty, $output:ty) => {
        pub async fn $name(&self, input: $input) -> Result<$output> {
            let mut client = self.client.clone();
            let request = Request::new(input);
            let response = client.$name(request).await?;
            Ok(response.into_inner())
        }
    };
}

impl Client {
    method!(list_buckets, ListBucketsRequest, ListBucketsResponse);

    method!(create_bucket, CreateBucketRequest, CreateBucketResponse);

    method!(delete_bucket, DeleteBucketRequest, DeleteBucketResponse);

    method!(list_objects, ListObjectsRequest, ListObjectsResponse);

    method!(upload_object, UploadObjectRequest, UploadObjectResponse);

    method!(delete_object, DeleteObjectRequest, DeleteObjectResponse);

    method!(show_object, ShowObjectRequest, ShowObjectResponse);

    method!(read_object, ReadObjectRequest, ReadObjectResponse);

    pub async fn connect(addr: &str) -> Result<Client> {
        let client = StorageClient::connect(addr.to_owned()).await?;
        Ok(Client { client })
    }
}
