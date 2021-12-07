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
    bucket::Bucket,
    client::Client,
    proto::{CreateBucketRequest, DeleteBucketRequest},
};
use crate::{async_trait, Result};

#[derive(Clone)]
pub struct Storage {
    client: Client,
}

impl Storage {
    pub async fn connect(addr: &str) -> Result<Storage> {
        let client = Client::connect(addr).await?;
        Ok(Storage { client })
    }
}

#[async_trait]
impl crate::Storage for Storage {
    type Bucket = Bucket;

    async fn bucket(&self, name: &str) -> Result<Self::Bucket> {
        Ok(Bucket::new(self.client.clone(), name))
    }

    async fn create_bucket(&self, name: &str) -> Result<Self::Bucket> {
        let input = CreateBucketRequest {
            bucket: name.to_owned(),
        };
        self.client.create_bucket(input).await?;
        self.bucket(name).await
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let input = DeleteBucketRequest {
            bucket: name.to_owned(),
        };
        self.client.delete_bucket(input).await?;
        Ok(())
    }
}
