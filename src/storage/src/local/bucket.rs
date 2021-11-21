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
    object::LocalObject,
};
use crate::{async_trait, Bucket, ObjectUploader};

pub struct LocalBucket {
    path: PathBuf,
}

impl LocalBucket {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn object_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path.join(name)
    }
}

#[async_trait]
impl Bucket<LocalObject> for LocalBucket {
    type ObjectUploader = LocalObjectUploader;

    async fn object(&self, name: &str) -> Result<LocalObject> {
        let path = self.object_path(name);
        if fs::metadata(&path).await.is_err() {
            return Err(Error::NotFound(name.to_owned()));
        }
        Ok(LocalObject::new(path))
    }

    async fn upload_object(&self, name: &str) -> Result<LocalObjectUploader> {
        let path = self.object_path(name);
        Ok(LocalObjectUploader::new(path))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        let path = self.object_path(name);
        fs::remove_file(path).await?;
        Ok(())
    }
}

pub struct LocalObjectUploader {
    path: PathBuf,
    buf: Vec<u8>,
}

impl<'a> LocalObjectUploader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            buf: vec![],
        }
    }
}

#[async_trait]
impl<'a> ObjectUploader for LocalObjectUploader {
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
            .open(&self.path)
            .await?;

        f.write_all(&self.buf).await?;
        f.sync_all().await?;

        Ok(1)
    }
}
