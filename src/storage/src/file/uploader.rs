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

use std::path::PathBuf;

use tokio::{fs, io::AsyncWriteExt};

use super::{
    error::{Error, Result},
    storage::check_io_result,
};
use crate::{async_trait, ObjectUploader};

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
        let mut f = check_io_result(
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.path)
                .await,
            &self.path,
        )
        .await?;

        f.write_all(&self.buf).await?;
        f.sync_all().await?;

        Ok(self.buf.len())
    }
}
