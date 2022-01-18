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
    mem_table::{MemTable, MemTableScanner},
    merging_scanner::MergingScanner,
    table::{TableReader, TableScanner},
};

#[allow(dead_code)]
pub struct Version {
    mem: MemVersion,
    base: BaseVersion,
}

#[allow(dead_code)]
impl Version {
    pub fn scan(&self) -> Scanner {
        let mem = self.mem.scan();
        let base = self.base.scan();
        Scanner::new(mem, base)
    }
}

/// Scans all entries in a [`Version`].
pub struct Scanner {
    _mem: MemScanner,
    _base: BaseScanner,
}

impl Scanner {
    pub fn new(_mem: MemScanner, _base: BaseScanner) -> Self {
        Self { _mem, _base }
    }
}

pub struct MemVersion {
    tables: Vec<Arc<MemTable>>,
}

/// Scans all tables in a [`MemVersion`].
type MemScanner = MergingScanner<MemTableScanner>;

impl MemVersion {
    pub fn scan(&self) -> MemScanner {
        let children = self.tables.iter().map(|x| x.scan()).collect();
        MergingScanner::new(children)
    }
}

pub struct BaseVersion {
    levels: Vec<LevelState>,
}

impl BaseVersion {
    pub fn scan(&self) -> BaseScanner {
        let children = self.levels.iter().map(|x| x.scan()).collect();
        BaseScanner::new(children)
    }
}

/// Scans all levels in a [`BaseVersion`].
pub struct BaseScanner {
    _children: Vec<LevelScanner>,
}

impl BaseScanner {
    pub fn new(_children: Vec<LevelScanner>) -> Self {
        Self { _children }
    }
}

pub struct LevelState {
    tables: Vec<TableReader>,
}

impl LevelState {
    pub fn scan(&self) -> LevelScanner {
        let children = self.tables.iter().map(|x| x.scan()).collect();
        LevelScanner::new(children)
    }
}

/// Scans all tables in a level.
pub struct LevelScanner {
    _children: Vec<TableScanner>,
}

impl LevelScanner {
    pub fn new(_children: Vec<TableScanner>) -> Self {
        Self { _children }
    }
}
