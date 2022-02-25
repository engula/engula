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

use std::{fs, path::PathBuf};

use super::Bucket;
use crate::{async_trait, Error, Result};

pub struct Tenant {
    path: PathBuf,
}

impl Tenant {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl crate::Tenant for Tenant {
    type Bucket = Bucket;

    fn bucket(&self, name: &str) -> Bucket {
        Bucket::new(self.path.join(name))
    }

    async fn create_bucket(&self, name: &str) -> Result<Bucket> {
        let path = self.path.join(name);
        if path.exists() {
            return Err(Error::AlreadyExists(format!("bucket {}", name)));
        }
        fs::create_dir_all(&path)?;
        Ok(self.bucket(name))
    }
}
