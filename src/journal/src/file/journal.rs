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

use std::collections::{hash_map, HashMap};
use std::path::{Path, PathBuf};

use tokio::sync::Mutex;
use tokio::{fs, io};

use super::{
    stream::Stream,
};

use crate::{async_trait, Timestamp, Error, Result};
use futures::StreamExt;
use std::ffi::{OsString, OsStr};

#[derive(Clone)]
pub struct Journal {
    streams: Mutex<HashMap<String, Stream>>,
    root: PathBuf,
}

impl Journal {
    pub async fn new(root: impl Into<PathBuf>) -> Result<Journal> {
        let path = root.into();

        match fs::DirBuilder::new().recursive(true).create(&path).await {
            io::Result::Ok(_) => {
                let mut journal = Journal {
                    root: path,
                    streams: Mutex::new(HashMap::new()),
                };
                journal.init();
                Ok(journal)
            }
            io::Result::Err(e) => Err(Error::Unknown(e.to_string())),
        }
    }

    fn stream_dir_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }

    async fn init(&self) {
        if let Ok(streams) = Journal::read_stream_dir(self.root.clone()).await {
            for stream in streams {
                let Some(name) = stream.file_name(); {
                    self.create_stream_internal(name.to_str().unwrap());
                }
            }
        }
    }

    async fn read_stream_dir(root: impl Into<PathBuf>) -> Result<Vec<PathBuf>> {
        let path = root.into();

        let mut stream_list: Vec<PathBuf> = fs::read_dir(path)
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_dir())
            .flat_map(|path| {
                path.file_name().into()
            })
            .flatten()
            .collect();

        Ok(stream_list)
    }

    async fn  create_stream_internal(&self, name: &str) -> Result<Stream> {
        let mut streams = self.streams.lock().await;
        match streams.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                let path = self.stream_dir_path(name);
                match Stream::new(path) {
                    Ok(stream) => {
                        ent.insert(stream.clone());
                        Ok(stream)
                    }
                    Err(e) => Result::Err(e)
                }
            }
            hash_map::Entry::Occupied(_) => Err(Error::AlreadyExists(format!("stream '{}'", name))),
        }
    }
}


#[async_trait]
impl crate::Journal for Journal {
    type Stream = Stream;

    async fn stream(&self, name: &str) -> Result<Stream> {
        let streams = self.streams.lock().await;
        match streams.get(name) {
            Some(stream) => Ok(stream.clone()),
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }

    async fn create_stream(&self, name: &str) -> Result<Stream> {
        self.create_stream_internal(name).await
    }

    async fn delete_stream(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.lock().await;
        match streams.remove(name) {
            Some(stream) => {
                stream.clean()?;
                Ok(())
            }
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }
}
