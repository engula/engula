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
    error::{Error, Result},
    stream::MemStream,
};

use crate::{async_trait, Journal, Timestamp};
use crate::file::FileStream;
use futures::StreamExt;
use std::ffi::{OsString, OsStr};


pub struct FileJournal<T: Timestamp> {
    streams: Mutex<HashMap<String, FileStream<T>>>,
    root: PathBuf,
}

impl<T: Timestamp> FileJournal<T> {
    pub fn new(root: impl Into<PathBuf>) -> Result<FileJournal<T>> {
        let path = root.into();

        match fs::DirBuilder::new().recursive(true).create(&path).await {
            Ok(_) => {
                let mut journal = FileJournal {
                    root: path,
                    streams: Mutex::new(HashMap::new()),
                };
                journal.init();
                Ok(journal)
            }
            Err(e) => Error::IO(e),
        }
    }

    fn stream_dir_path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root.join(name)
    }

    fn init(&self) {
        let streams = read_stream_dir(self.root.clone())?;
        for &stream in &streams {
            self.create_stream(stream)
        }
    }

    fn read_stream_dir(root: impl Into<PathBuf>) -> Result<Vec<str>> {
        let path = root.into();

        let mut stream_list: Vec<str> = fs::read_dir(path)?
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_dir())
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
            })
            .flatten()
            .collect();

        Ok(stream_list)
    }
}


#[async_trait]
impl<T: Timestamp> Journal<FileStream<T>> for FileJournal<T> {

    async fn stream(&self, name: &str) -> Result<FileStream<T>> {
        let streams = self.streams.lock().await;
        match streams.get(name) {
            Some(stream) => Ok(stream.clone()),
            None => Err(Error::NotFound(format!("stream '{}'", name))),
        }
    }

    async fn create_stream(&self, name: &str) -> Result<FileStream<T>> {
        let mut streams = self.streams.lock().await;
        match streams.entry(name.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                let path = self.stream_dir_path(name);
                match FileStream::new(path) {
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
