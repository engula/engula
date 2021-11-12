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

use futures::{future, stream};

use super::{
    bucket::RemoteBucket,
    client::Client,
    proto::{CreateBucketRequest, DeleteBucketRequest, ListBucketsRequest},
};
use crate::{async_trait, Bucket, Result, ResultStream, Storage};

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
impl Storage for RemoteStorage {
    async fn bucket(&self, name: &str) -> Result<Box<dyn Bucket>> {
        let bucket = RemoteBucket::new(self.client.clone(), name.to_owned());
        Ok(Box::new(bucket))
    }

    async fn list_buckets(&self) -> ResultStream<String> {
        let input = ListBucketsRequest {};
        match self.client.list_buckets(input).await {
            Ok(output) => {
                let bucket_names = output
                    .buckets
                    .into_iter()
                    .map(Ok)
                    .collect::<Vec<Result<String>>>();
                Box::new(stream::iter(bucket_names))
            }
            Err(err) => Box::new(stream::once(future::err(err.into()))),
        }
    }

    async fn create_bucket(&self, name: &str) -> Result<Box<dyn Bucket>> {
        let input = CreateBucketRequest {
            bucket: name.to_owned(),
        };
        let _ = self.client.create_bucket(input).await?;
        self.bucket(name).await
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let input = DeleteBucketRequest {
            bucket: name.to_owned(),
        };
        let _ = self.client.delete_bucket(input).await?;
        Ok(())
    }
}
