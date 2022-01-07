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

use std::{path::PathBuf, sync::Arc};

use tokio::{fs, sync::Mutex};

use super::stream::Stream;
use crate::{async_trait, Error, Result};

#[derive(Clone)]
pub struct Journal {
    root: Arc<Mutex<PathBuf>>,
    segment_size: usize,
}

impl Journal {
    pub async fn open(root: impl Into<PathBuf>, segment_size: usize) -> Result<Self> {
        let root = root.into();
        Ok(Self {
            root: Arc::new(Mutex::new(root)),
            segment_size,
        })
    }
}

#[async_trait]
impl crate::Journal for Journal {
    type Stream = Stream;

    async fn stream(&self, name: &str) -> Result<Stream> {
        let root = self.root.lock().await;
        let path = root.join(name);
        if !path.exists() {
            return Err(Error::NotFound(format!("stream '{:?}'", path)));
        }
        Stream::open(path, self.segment_size).await
    }

    async fn create_stream(&self, name: &str) -> Result<Stream> {
        let root = self.root.lock().await;
        let path = root.join(name);
        if path.exists() {
            return Err(Error::AlreadyExists(format!("stream '{:?}'", path)));
        }
        fs::create_dir_all(&path).await?;
        Stream::open(path, self.segment_size).await
    }

    async fn delete_stream(&self, name: &str) -> Result<()> {
        let root = self.root.lock().await;
        let path = root.join(name);
        if !path.exists() {
            return Err(Error::NotFound(format!("stream '{:?}'", path)));
        }
        fs::remove_dir_all(path).await?;
        Ok(())
    }
}
