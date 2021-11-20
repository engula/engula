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

use tokio::{fs, io::AsyncWriteExt};

use super::{
    error::{Error, Result},
    object::FsObject,
};
use crate::{async_trait, Bucket, ObjectUploader};

pub struct FsBucket {
    path: PathBuf,
}

impl FsBucket {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
        }
    }

    fn object_path(&self, name: impl Into<String>) -> PathBuf {
        let mut obj_path = self.path.to_owned();
        obj_path.push(name.into());
        obj_path
    }
}

#[async_trait]
impl Bucket<FsObject> for FsBucket {
    type ObjectUploader = FsObjectUploader;

    async fn object(&self, name: &str) -> Result<FsObject> {
        let path = self.object_path(name.to_owned());
        if fs::metadata(path.as_path()).await.is_err() {
            return Err(Error::NotFound(name.to_owned()));
        }
        Ok(FsObject::new(path))
    }

    async fn upload_object(&self, name: &str) -> Result<FsObjectUploader> {
        let path = self.object_path(name.to_owned());
        Ok(FsObjectUploader::new(path))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        let path = self.object_path(name.to_owned());
        fs::remove_file(path.as_path()).await?;
        Ok(())
    }
}

pub struct FsObjectUploader {
    path: PathBuf,
    buf: Vec<u8>,
}

impl FsObjectUploader {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().into(),
            buf: vec![],
        }
    }
}

#[async_trait]
impl ObjectUploader for FsObjectUploader {
    type Error = Error;

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(buf);
        Ok(())
    }

    async fn finish(self) -> Result<usize> {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.path.as_path())
            .await?;

        let v: &[u8] = &self.buf;
        f.write_all(v).await?;
        f.sync_all().await?;

        Ok(1)
    }
}
