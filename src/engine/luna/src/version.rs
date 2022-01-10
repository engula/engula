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

use engula_futures::io::RandomRead;

use crate::{
    mem_table::MemTable,
    merging_scanner::MergingScanner,
    table::{TableIter, TableReader},
};

#[allow(dead_code)]
pub struct Version<M, R> {
    mem: MemVersion<M>,
    base: BaseVersion<R>,
}

#[allow(dead_code)]
impl<M, R> Version<M, R>
where
    M: MemTable,
    R: RandomRead + Unpin,
{
    pub fn scan(&self) -> Scanner<'_, M::Scanner, R> {
        let mem = self.mem.scan();
        let base = self.base.scan();
        Scanner::new(mem, base)
    }
}

/// Scans all entries in a [`Version`].
pub struct Scanner<'a, S, R> {
    _mem: MemScanner<S>,
    _base: BaseScanner<'a, R>,
}

impl<'a, S, R> Scanner<'a, S, R> {
    pub fn new(_mem: MemScanner<S>, _base: BaseScanner<'a, R>) -> Self {
        Self { _mem, _base }
    }
}

pub struct MemVersion<M> {
    tables: Vec<Arc<M>>,
}

/// Scans all tables in a [`MemVersion`].
type MemScanner<S> = MergingScanner<S>;

impl<M> MemVersion<M>
where
    M: MemTable,
{
    pub fn scan(&self) -> MergingScanner<M::Scanner> {
        let children = self.tables.iter().map(|x| x.scan()).collect();
        MergingScanner::new(children)
    }
}

pub struct BaseVersion<R> {
    levels: Vec<LevelState<R>>,
}

impl<R> BaseVersion<R>
where
    R: RandomRead + Unpin,
{
    pub fn scan(&self) -> BaseScanner<'_, R> {
        let children = self.levels.iter().map(|x| x.scan()).collect();
        BaseScanner::new(children)
    }
}

/// Scans all levels in a [`BaseVersion`].
pub struct BaseScanner<'a, R> {
    _children: Vec<LevelScanner<'a, R>>,
}

impl<'a, R> BaseScanner<'a, R> {
    pub fn new(_children: Vec<LevelScanner<'a, R>>) -> Self {
        Self { _children }
    }
}

pub struct LevelState<R> {
    tables: Vec<TableReader<R>>,
}

impl<R> LevelState<R>
where
    R: RandomRead + Unpin,
{
    pub fn scan(&self) -> LevelScanner<'_, R> {
        let children = self.tables.iter().map(|x| x.iter()).collect();
        LevelScanner::new(children)
    }
}

/// Scans all tables in a level.
pub struct LevelScanner<'a, R> {
    _children: Vec<TableIter<'a, R>>,
}

impl<'a, R> LevelScanner<'a, R> {
    pub fn new(_children: Vec<TableIter<'a, R>>) -> Self {
        Self { _children }
    }
}
