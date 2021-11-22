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

use tokio::{fs, io};

use super::{
    bucket::FileObjectUploader,
    error::{Error, Result},
    object::FileObject,
};
use crate::{async_trait, Storage};

pub struct FileStorage {
    root: PathBuf,
}

impl FileStorage {
    pub async fn new(root: impl Into<PathBuf>) -> Result<FileStorage> {
        let path = root.into();
        fs::DirBuilder::new().recursive(true).create(&path).await?;
        Ok(Self { root: path })
    }

    fn bucket_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }

    fn object_path(&self, bucket_name: impl AsRef<Path>, object_name: impl AsRef<Path>) -> PathBuf {
        self.root.join(bucket_name).join(object_name)
    }
}

#[async_trait]
impl Storage<FileObject> for FileStorage {
    type ObjectUploader = FileObjectUploader;

    async fn create_bucket(&self, name: &str) -> Result<()> {
        let path = self.bucket_path(name);

        if try_exists(&path).await? {
            return Err(Error::AlreadyExists(name.to_owned()));
        }

        fs::create_dir_all(&path).await?;

        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let path = self.bucket_path(name);

        fs::remove_dir(path).await?;

        Ok(())
    }

    async fn object(&self, bucket_name: &str, object_name: &str) -> Result<FileObject> {
        let path = self.object_path(bucket_name, object_name);
        if !try_exists(&path).await? {
            return Err(Error::NotFound(object_name.to_owned()));
        }
        Ok(FileObject::new(path))
    }

    async fn upload_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<FileObjectUploader> {
        let path = self.object_path(bucket_name, object_name);
        Ok(FileObjectUploader::new(path))
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        let path = self.object_path(bucket_name, object_name);
        fs::remove_file(path).await?;
        Ok(())
    }
}

// async version for `std:fs:try_exist`, remove me after https://github.com/tokio-rs/tokio/pull/3375 addressed.
pub async fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}
