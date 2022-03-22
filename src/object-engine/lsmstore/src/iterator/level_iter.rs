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

use std::sync::Arc;

use object_engine_filestore::Store as FileStore;

use crate::{iterator::ManifestIter, versions::FileMetadata, *};

pub struct LevelIter {
    tenant: String,
    bucket: String,
    local_store: Arc<dyn FileStore>,

    manifest_file_iter: ManifestIter,
    current_file: Option<FileMetadata>,
    current_iter: Option<TableIter>,

    init: bool,
}

impl LevelIter {
    pub async fn new(
        tenant: &str,
        bucket: &str,
        manifest_file_iter: ManifestIter,
        local_store: Arc<dyn FileStore>,
    ) -> Result<Self> {
        let mut iter = Self {
            tenant: tenant.to_owned(),
            bucket: bucket.to_owned(),
            manifest_file_iter,
            current_file: None,
            current_iter: None,
            local_store,
            init: false,
        };
        iter.seek_to_first().await?;
        Ok(iter)
    }

    pub fn key(&self) -> Option<Key<'_>> {
        debug_assert!(self.valid());
        self.current_iter.as_ref().map(|e| e.key())
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        self.current_iter.as_ref().unwrap().value()
    }

    pub fn valid(&self) -> bool {
        !self.init
            || (self.current_file.is_some()
                && self.current_iter.is_some()
                && self.current_iter.as_ref().unwrap().valid())
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.manifest_file_iter.seek_to_first();
        if !self.manifest_file_iter.valid() {
            return Ok(());
        }
        self.current_file = Some(self.manifest_file_iter.value());
        let file_metadata = self.current_file.as_ref().unwrap();
        self.current_iter = Some(
            self.open_table_iter(&file_metadata.name, file_metadata.file_size as usize)
                .await?,
        );
        self.current_iter.as_mut().unwrap().seek_to_first().await?;
        if !self.init {
            self.init = true;
        }
        Ok(())
    }

    pub async fn seek(&mut self, target: Key<'_>) -> Result<()> {
        self.manifest_file_iter.seek(target);
        if !self.manifest_file_iter.valid() {
            return Ok(());
        }
        self.set_current_tbl_iter().await?;
        self.current_iter.as_mut().unwrap().seek(target).await?;
        if !self.init {
            self.init = true;
        }
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if !self.init {
            self.manifest_file_iter.seek_to_first();
            if !self.manifest_file_iter.valid() {
                return Ok(());
            }
            self.set_current_tbl_iter().await?;
            self.current_iter.as_mut().unwrap().seek_to_first().await?;
        } else if !self.current_iter.as_ref().unwrap().valid() {
            self.manifest_file_iter.next();
            if !self.manifest_file_iter.valid() {
                return Ok(());
            }
            self.set_current_tbl_iter().await?;
            self.current_iter.as_mut().unwrap().seek_to_first().await?;
        } else {
            self.current_iter.as_mut().unwrap().next().await?;
        }
        Ok(())
    }

    async fn set_current_tbl_iter(&mut self) -> Result<()> {
        self.current_file = Some(self.manifest_file_iter.value());
        let f = self.current_file.as_ref().unwrap();
        self.current_iter = Some(self.open_table_iter(&f.name, f.file_size as usize).await?);
        Ok(())
    }

    async fn open_table_iter(&self, file: &str, file_size: usize) -> Result<TableIter> {
        let r = self
            .local_store
            .tenant(&self.tenant)
            .bucket(&self.bucket)
            .new_random_reader(file)
            .await?;
        let tr = TableReader::open(r.into(), file_size).await?;
        Ok(tr.iter())
    }
}
