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

use std::path::Path;

use super::{manifest::Manifest, Journal, Storage};
use crate::Result;

pub type Kernel = crate::local::Kernel<Journal, Storage, Manifest>;

const SEGMENT_SIZE: usize = 64 * 1024 * 1024;

impl Kernel {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let journal_path = path.join("journal");
        let storage_path = path.join("storage");
        let manifest_path = path.join("MANIFEST");
        let journal = Journal::open(journal_path, SEGMENT_SIZE).await?;
        let storage = Storage::new(storage_path).await?;
        let manifest = Manifest::open(manifest_path).await;
        Self::init(journal, storage, manifest).await
    }
}
