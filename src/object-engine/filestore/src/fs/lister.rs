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

use tokio::fs::ReadDir;

use crate::{async_trait, Error, FileDesc, Result};

pub struct DirLister {
    dir: ReadDir,
}

impl DirLister {
    pub fn new(dir: ReadDir) -> Self {
        Self { dir }
    }
}

#[async_trait]
impl crate::Lister for DirLister {
    type Item = String;

    async fn next(&mut self, n: usize) -> Result<Vec<Self::Item>> {
        let mut result = Vec::new();
        for _i in 0..n {
            if let Some(ent) = self.dir.next_entry().await? {
                let file_name = ent
                    .file_name()
                    .into_string()
                    .map_err(|s| Error::Corrupted(format!("invalid name {:?}", s)))?;
                result.push(file_name);
            } else {
                break;
            }
        }
        Ok(result)
    }
}

pub struct FileLister {
    dir: ReadDir,
}

impl FileLister {
    pub fn new(dir: ReadDir) -> Self {
        Self { dir }
    }
}

#[async_trait]
impl crate::Lister for FileLister {
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
