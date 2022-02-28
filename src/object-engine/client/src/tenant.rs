// Copyright 2022 The Engula Authors.
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

use object_engine_master::{proto::*, Master};

use crate::{Bucket, Result};

#[derive(Clone)]
pub struct Tenant {
    id: u64,
    master: Master,
}

impl Tenant {
    pub fn new(id: u64, master: Master) -> Self {
        Self { id, master }
    }

    pub fn bucket(&self, id: u64) -> Bucket {
        Bucket::new(id, self.id, self.master.clone())
    }

    pub async fn create_bucket(&self, name: &str) -> Result<BucketDesc> {
        let desc = BucketDesc {
            name: name.to_owned(),
            ..Default::default()
        };
        self.master.create_bucket(self.id, desc).await
    }
}
