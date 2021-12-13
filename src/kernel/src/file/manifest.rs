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

use std::{io::ErrorKind, path::PathBuf};

use prost::Message;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{async_trait, Error, Result, Version};

#[derive(Clone)]
pub struct Manifest {
    path: PathBuf,
}

impl Manifest {
    pub async fn open(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl crate::manifest::Manifest for Manifest {
    async fn load_version(&self) -> Result<Version> {
        let mut buf = Vec::new();
        match File::open(&self.path).await {
            Ok(mut file) => {
                file.read_to_end(&mut buf).await?;
                Version::decode(buf.as_ref()).map_err(|err| Error::Corrupted(err.to_string()))
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    Ok(Version::default())
                } else {
                    Err(err.into())
                }
            }
        }
    }

    async fn save_version(&self, version: &Version) -> Result<()> {
        let buf = version.encode_to_vec();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)
            .await?;
        file.write_buf(&mut buf.as_ref()).await?;
        file.sync_data().await?;
        Ok(())
    }
}
