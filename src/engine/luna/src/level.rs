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

use std::sync::Arc;

use crate::{
    codec::{ParsedInternalKey, TableDesc},
    table::{TableReader, TableScanner},
    Result,
};

#[derive(Clone, Default)]
pub struct LevelState {
    pub tables: Vec<Arc<TableState>>,
}

impl LevelState {
    pub fn scan(&self) -> LevelScanner {
        LevelScanner::new(self.tables.clone())
    }
}

pub struct TableState {
    pub desc: TableDesc,
    pub reader: TableReader,
}

/// Scans all tables in a level.
pub struct LevelScanner {
    tables: Vec<Arc<TableState>>,
    scanners: Vec<TableScanner>,
    current: usize,
}

impl LevelScanner {
    pub fn new(tables: Vec<Arc<TableState>>) -> Self {
        let scanners = tables.iter().map(|x| x.reader.scan()).collect();
        Self {
            tables,
            scanners,
            current: 0,
        }
    }

    async fn skip_forward_until_valid(&mut self) -> Result<()> {
        while let Some(s) = self.scanners.get(self.current) {
            if s.valid() {
                break;
            }
            self.current += 1;
            if let Some(s) = self.scanners.get_mut(self.current) {
                s.seek_to_first().await?;
            }
        }
        Ok(())
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.current = 0;
        if let Some(s) = self.scanners.get_mut(self.current) {
            s.seek_to_first().await?;
        }
        self.skip_forward_until_valid().await
    }

    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        let target_pk = ParsedInternalKey::decode_from(target);
        match self.tables.binary_search_by(|x| {
            let lower_bound_pk = ParsedInternalKey::decode_from(&x.desc.lower_bound);
            target_pk.cmp(&lower_bound_pk)
        }) {
            Ok(index) => {
                self.current = index;
                self.scanners[index].seek(target).await?;
                self.skip_forward_until_valid().await
            }
            Err(_) => {
                self.current = self.scanners.len();
                Ok(())
            }
        }
    }

    pub async fn next(&mut self) -> Result<()> {
        if let Some(s) = self.scanners.get_mut(self.current) {
            s.next().await?;
        }
        self.skip_forward_until_valid().await
    }

    pub fn valid(&self) -> bool {
        if let Some(s) = self.scanners.get(self.current) {
            s.valid()
        } else {
            false
        }
    }

    pub fn key(&self) -> &[u8] {
        self.scanners[self.current].key()
    }

    pub fn value(&self) -> &[u8] {
        self.scanners[self.current].value()
    }
}
