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

use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::Hasher,
    io::Result,
};

mod file;
use file::{ActiveFile, SealedFile};

mod table;
use table::{DiskHandle, DiskTable};

pub struct DiskCache {
    index: HashMap<u64, u32>,
    table: DiskTable,
    active_file: ActiveFile,
    sealed_files: Vec<SealedFile>,
    oldest_fileno: u32,
}

impl DiskCache {
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let hash = hash(key);
        todo!()
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    async fn read_from_disk(&self, handle: &DiskHandle) -> Result<Option<Vec<u8>>> {
        if handle.fileno < self.oldest_fileno {
            return Ok(None);
        }
        let offset = handle.fileno - self.oldest_fileno;
        let active_fileno = self.sealed_files.len() as u32;
        assert!(offset <= active_fileno);
        if offset == active_fileno {
            self.active_file
                .read(handle.offset, handle.length)
                .await
                .map(Some)
        } else {
            self.sealed_files[offset as usize]
                .read(handle.offset, handle.length)
                .await
                .map(Some)
        }
    }
}

fn hash(data: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::default();
    hasher.write(data);
    hasher.finish()
}
