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

use std::{os::unix::fs::FileExt, path::PathBuf};

use tokio::{fs, io::AsyncWriteExt};

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
    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let path = self.path.join(bucket);
        if path.exists() {
            return Err(Error::AlreadyExists(format!("bucket {}", bucket)));
        }
        fs::create_dir_all(&path).await?;
        Ok(())
    }

    async fn new_random_reader(
        &self,
        bucket: &str,
        file_name: &str,
    ) -> Result<Box<dyn crate::RandomRead>> {
        let path = self.path.join(bucket).join(file_name);
        let file = tokio::fs::File::open(&path).await?;
        Ok(Box::new(RandomReader {
            file: file.into_std().await,
        }))
    }

    async fn new_sequential_writer(
        &self,
        bucket: &str,
        file_name: &str,
    ) -> Result<Box<dyn crate::SequentialWrite>> {
        let path = self.path.join(bucket).join(file_name);
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        Ok(Box::new(SequentialWriter { file }))
    }
}

pub struct RandomReader {
    file: std::fs::File,
}

#[async_trait]
impl crate::RandomRead for RandomReader {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.file.read_exact_at(buf, offset)?;
        Ok(())
    }
}

pub struct SequentialWriter {
    file: tokio::fs::File,
}

#[async_trait]
impl crate::SequentialWrite for SequentialWriter {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.file.write_all(buf).await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }
}
