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
    base_version::{BaseScanner, BaseVersion},
    codec::ParsedInternalKey,
    memtable::{Memtable, MemtableScanner},
    merging_scanner::MergingScanner,
    scan::Scan,
    ReadOptions, Result,
};

#[derive(Clone, Default)]
pub struct Version {
    pub mem: MemVersion,
    pub base: BaseVersion,
}

impl Version {
    pub async fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.mem.get(opts, key) {
            Ok(Some(value))
        } else {
            self.base.get(opts, key).await
        }
    }

    pub fn scan(&self, opts: &ReadOptions) -> InternalScanner {
        let mem = self.mem.scan(opts);
        let base = self.base.scan(opts);
        InternalScanner::new(mem, base)
    }
}

#[derive(Clone, Default)]
pub struct MemVersion {
    pub tables: Vec<Arc<Memtable>>,
}

/// Scans all tables in a [`MemVersion`].
type MemScanner = MergingScanner<MemtableScanner>;

impl MemVersion {
    pub fn get(&self, opts: &ReadOptions, key: &[u8]) -> Option<Vec<u8>> {
        for table in self.tables.iter().rev() {
            if let Some(value) = table.get(opts.snapshot.ts, key) {
                return Some(value);
            }
        }
        None
    }

    pub fn scan(&self, opts: &ReadOptions) -> MemScanner {
        let children = self
            .tables
            .iter()
            .map(|x| x.scan(opts.snapshot.ts))
            .collect();
        MergingScanner::new(children)
    }
}

#[derive(Debug)]
enum CurrentScanner {
    Mem,
    Base,
    None,
}

/// Scans all entries in a [`Version`].
pub struct InternalScanner {
    mem: MemScanner,
    base: BaseScanner,
    current: CurrentScanner,
}

impl InternalScanner {
    pub fn new(mem: MemScanner, base: BaseScanner) -> Self {
        Self {
            mem,
            base,
            current: CurrentScanner::None,
        }
    }

    fn set_current_scanner(&mut self) {
        if self.mem.valid() && self.base.valid() {
            let mem_pk = ParsedInternalKey::decode_from(self.mem.key());
            let base_pk = ParsedInternalKey::decode_from(self.base.key());
            if mem_pk < base_pk {
                self.current = CurrentScanner::Mem;
            } else {
                self.current = CurrentScanner::Base;
            }
        } else if self.mem.valid() {
            self.current = CurrentScanner::Mem;
        } else if self.base.valid() {
            self.current = CurrentScanner::Base;
        } else {
            self.current = CurrentScanner::None;
        }
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.mem.seek_to_first();
        self.base.seek_to_first().await?;
        self.set_current_scanner();
        Ok(())
    }

    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.mem.seek(target);
        self.base.seek(target).await?;
        self.set_current_scanner();
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        match self.current {
            CurrentScanner::Mem => self.mem.next(),
            CurrentScanner::Base => self.base.next().await?,
            CurrentScanner::None => {}
        }
        self.set_current_scanner();
        Ok(())
    }

    pub fn valid(&self) -> bool {
        match self.current {
            CurrentScanner::Mem => self.mem.valid(),
            CurrentScanner::Base => self.base.valid(),
            CurrentScanner::None => false,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self.current {
            CurrentScanner::Mem => self.mem.key(),
            CurrentScanner::Base => self.base.key(),
            CurrentScanner::None => panic!(),
        }
    }

    pub fn value(&self) -> &[u8] {
        match self.current {
            CurrentScanner::Mem => self.mem.value(),
            CurrentScanner::Base => self.base.value(),
            CurrentScanner::None => panic!(),
        }
    }
}
