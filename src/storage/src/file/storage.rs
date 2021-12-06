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

use std::path::{Path, PathBuf};

use tokio::fs;

use super::bucket::{try_exists, Bucket};
use crate::{async_trait, Error, Result};

#[derive(Clone)]
pub struct Storage {
    root: PathBuf,
}

impl Storage {
    pub async fn new(root: impl Into<PathBuf>) -> Result<Storage> {
        let path = root.into();
        fs::DirBuilder::new().recursive(true).create(&path).await?;
        Ok(Self { root: path })
    }

    fn bucket_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }
}

#[async_trait]
impl crate::Storage for Storage {
    type Bucket = Bucket;

    async fn bucket(&self, name: &str) -> Result<Self::Bucket> {
        let path = self.bucket_path(name);
        Ok(Bucket::new(path))
    }

    async fn create_bucket(&self, name: &str) -> Result<Self::Bucket> {
        let path = self.bucket_path(name);

        if try_exists(&path).await? {
            return Err(Error::AlreadyExists(format!("bucket '{}'", name)));
        }

        fs::create_dir_all(&path).await?;

        self.bucket(name).await
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let path = self.bucket_path(name);

        fs::remove_dir(path).await?;

        Ok(())
    }
}
