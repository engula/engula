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

use object_engine_master::proto::*;

use crate::{Master, Result};

#[allow(dead_code)]
#[derive(Clone)]
pub struct Bucket {
    name: String,
    tenant: String,
    master: Master,
}

impl Bucket {
    pub(crate) fn new(name: String, tenant: String, master: Master) -> Self {
        Self {
            name,
            tenant,
            master,
        }
    }

    pub async fn desc(&self) -> Result<BucketDesc> {
        self.master
            .describe_bucket(self.tenant.clone(), self.name.clone())
            .await
    }
}
