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

use super::{
    bucket::LocalBucket,
    error::{Error, Result},
    object::LocalObject,
};
use crate::{async_trait, Storage};

pub struct LocalStorage {
    root: PathBuf,
}

impl<'a> LocalStorage {
    pub async fn new(root: impl Into<PathBuf>) -> Result<LocalStorage> {
        let path = root.into();
        fs::DirBuilder::new().recursive(true).create(&path).await?;
        Ok(Self { root: path })
    }

    fn bucket_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }
}

#[async_trait]
impl Storage<LocalObject, LocalBucket> for LocalStorage {
    async fn bucket(&self, name: &str) -> Result<LocalBucket> {
        let path = self.bucket_path(name);

        if fs::metadata(&path).await.is_err() {
            return Err(Error::NotFound(name.to_owned()));
        }

        Ok(LocalBucket::new(path))
    }

    async fn create_bucket(&self, name: &str) -> Result<LocalBucket> {
        let path = self.bucket_path(name);

        if fs::metadata(&path).await.is_ok() {
            return Err(Error::AlreadyExists(name.to_owned()));
        }

        fs::create_dir_all(&path).await?;

        Ok(LocalBucket::new(path))
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let path = self.bucket_path(name);

        fs::remove_dir(path).await?;

        Ok(())
    }
}
