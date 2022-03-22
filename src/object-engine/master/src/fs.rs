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

use std::{path::PathBuf, sync::Arc};

use object_engine_filestore::{fs, Store};

use crate::Result;

pub type FileStore = Arc<dyn Store>;

pub async fn open(path: impl Into<PathBuf>) -> Result<FileStore> {
    let store = fs::Store::open(path).await?;
    let store: Box<dyn Store> = Box::new(store);
    Ok(store.into())
}
