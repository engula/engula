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

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{async_trait, Error, FileDesc, Result};

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
    async fn list_files(&self) -> Result<Box<dyn crate::Lister<Item = FileDesc>>> {
        let dir = tokio::fs::read_dir(&self.path).await?;
        Ok(Box::new(Lister { dir }))
    }

    async fn new_random_reader(&self, name: &str) -> Result<Box<dyn crate::RandomRead>> {
        let path = self.path.join(name);
        let file = tokio::fs::File::open(&path).await?;
        Ok(Box::new(RandomReader {
            file: file.into_std().await,
        }))
    }

    async fn new_sequential_reader(&self, name: &str) -> Result<Box<dyn crate::SequentialRead>> {
        let path = self.path.join(name);
        let file = tokio::fs::File::open(&path).await?;
        Ok(Box::new(SequentialReader { file }))
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn crate::SequentialWrite>> {
        let path = self.path.join(name);
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        Ok(Box::new(SequentialWriter { file }))
    }
}

struct Lister {
    dir: tokio::fs::ReadDir,
}

#[async_trait]
impl crate::Lister for Lister {
    type Item = FileDesc;

    async fn next(&mut self, n: usize) -> Result<Vec<Self::Item>> {
        let mut result = Vec::new();
        for _i in 0..n {
            if let Some(ent) = self.dir.next_entry().await? {
                let file_name = ent
                    .file_name()
                    .into_string()
                    .map_err(|s| Error::Corrupted(format!("invalid name {:?}", s)))?;
                let metadata = ent.metadata().await?;
                let desc = FileDesc {
                    name: file_name,
                    size: metadata.len() as usize,
                };
                result.push(desc);
            } else {
                break;
            }
        }
        Ok(result)
    }
}

pub struct RandomReader {
    file: std::fs::File,
}

#[async_trait]
impl crate::RandomRead for RandomReader {
    async fn read_at(&self, buf: &mut [u8], offset: usize) -> Result<usize> {
        let size = self.file.read_at(buf, offset as u64)?;
        Ok(size)
    }

    async fn read_exact_at(&self, buf: &mut [u8], offset: usize) -> Result<()> {
        self.file.read_exact_at(buf, offset as u64)?;
        Ok(())
    }
}

pub struct SequentialReader {
    file: tokio::fs::File,
}

#[async_trait]
impl crate::SequentialRead for SequentialReader {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let size = self.file.read(buf).await?;
        Ok(size)
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact(buf).await?;
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
