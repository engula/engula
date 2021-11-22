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
    object::FileObject,
    storage::try_exists,
};
use crate::{async_trait, Bucket, ObjectUploader};

pub struct FileBucket {
    path: PathBuf,
}

impl FileBucket {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn object_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path.join(name)
    }
}

#[async_trait]
impl Bucket<FileObject> for FileBucket {
    type ObjectUploader = FileObjectUploader;

    async fn object(&self, name: &str) -> Result<FileObject> {
        let path = self.object_path(name);
        if !try_exists(&path).await? {
            return Err(Error::NotFound(name.to_owned()));
        }
        Ok(FileObject::new(path))
    }

    async fn upload_object(&self, name: &str) -> Result<FileObjectUploader> {
        let path = self.object_path(name);
        Ok(FileObjectUploader::new(path))
    }

    async fn delete_object(&self, name: &str) -> Result<()> {
        let path = self.object_path(name);
        fs::remove_file(path).await?;
        Ok(())
    }
}

pub struct FileObjectUploader {
    path: PathBuf,
    buf: Vec<u8>,
}

impl FileObjectUploader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            buf: vec![],
        }
    }
}

#[async_trait]
impl ObjectUploader for FileObjectUploader {
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

        Ok(self.buf.len())
    }
}
