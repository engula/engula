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

use tokio::{fs, io};

use crate::{async_trait, Error, Result};

#[derive(Clone)]
pub struct Bucket {
    path: PathBuf,
}

impl Bucket {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn object_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path.join(name)
    }
}

#[async_trait]
impl crate::Bucket for Bucket {
    type SequentialReader = fs::File;
    type SequentialWriter = fs::File;

    async fn delete_object(&self, name: &str) -> Result<()> {
        let path = self.object_path(name);
        check_io_result(fs::remove_file(&path).await, &path).await?;
        Ok(())
    }

    async fn new_sequential_reader(&self, name: &str) -> Result<Self::SequentialReader> {
        let path = self.object_path(name);
        let f = check_io_result(fs::OpenOptions::new().read(true).open(&path).await, &path).await?;
        Ok(f)
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter> {
        let path = self.object_path(name);
        let f = check_io_result(
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .await,
            &path,
        )
        .await?;
        Ok(f)
    }
}

async fn check_io_result<T>(r: io::Result<T>, obj_path: impl AsRef<Path>) -> Result<T> {
    match r {
        Ok(t) => Ok(t),
        Err(err) => {
            if err.kind() == io::ErrorKind::NotFound {
                let parent = obj_path.as_ref().parent().unwrap();
                if !try_exists(parent).await? {
                    return Err(Error::NotFound(format!(
                        "bucket '{}'",
                        parent.file_name().unwrap().to_str().unwrap(),
                    )));
                }
                return Err(Error::NotFound(format!(
                    "object '{}'",
                    obj_path.as_ref().file_name().unwrap().to_str().unwrap(),
                )));
            }
            Err(err.into())
        }
    }
}

// async version for `std:fs:try_exist`, remove me after https://github.com/tokio-rs/tokio/pull/3375 addressed.
pub async fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}
