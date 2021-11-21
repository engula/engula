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

use std::{io::SeekFrom, path::PathBuf};

use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::error::{Error, Result};
use crate::{async_trait, Object};

pub struct FileObject {
    path: PathBuf,
}

impl<'a> FileObject {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl Object for FileObject {
    type Error = Error;

    async fn read_at(&self, mut buf: &mut [u8], offset: usize) -> Result<usize> {
        let mut f: fs::File = fs::OpenOptions::new().read(true).open(&self.path).await?;

        f.seek(SeekFrom::Start(offset as u64)).await?;

        let mut read_size: usize = 0;
        while !buf.is_empty() {
            let n = f.read(buf).await?;
            if n == 0 {
                break;
            }
            read_size += n;
            let tmp = buf;
            buf = &mut tmp[n..];
        }

        Ok(read_size)
    }
}
