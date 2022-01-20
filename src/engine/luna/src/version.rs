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
    memtable::{Memtable, MemtableScanner},
    merging_scanner::MergingScanner,
    ReadOptions,
};

#[derive(Clone, Default)]
pub struct Version {
    pub mem: MemVersion,
    pub base: BaseVersion,
}

impl Version {
    pub fn scan(&self, opts: &ReadOptions) -> Scanner {
        let mem = self.mem.scan(opts);
        let base = self.base.scan(opts);
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

#[derive(Clone, Default)]
pub struct MemVersion {
    pub tables: Vec<Arc<Memtable>>,
}

/// Scans all tables in a [`MemVersion`].
type MemScanner = MergingScanner<MemtableScanner>;

impl MemVersion {
    pub fn scan(&self, opts: &ReadOptions) -> MemScanner {
        let children = self
            .tables
            .iter()
            .map(|x| x.scan(opts.snapshot.ts))
            .collect();
        MergingScanner::new(children)
    }
}
