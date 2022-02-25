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

use tokio::io::AsyncWriteExt;

use crate::{async_trait, Result};

pub struct Bucket {
    path: PathBuf,
}

impl Bucket {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl crate::Bucket for Bucket {
    type RandomReader = RandomReader;
    type SequentialWriter = SequentialWriter;

    async fn new_random_reader(&self, name: &str) -> Result<Self::RandomReader> {
        let path = self.path.join(name);
        let file = tokio::fs::File::open(&path).await?;
        Ok(RandomReader {
            file: file.into_std().await,
        })
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter> {
        let path = self.path.join(name);
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        Ok(SequentialWriter { file })
    }
}

pub struct RandomReader {
    file: std::fs::File,
}

#[async_trait]
impl crate::RandomRead for RandomReader {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let size = self.file.read_at(buf, offset)?;
        Ok(size)
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

    async fn flush(&mut self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }
}
