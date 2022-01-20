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

use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    level::{LevelScanner, LevelState},
    ReadOptions, Result,
};

#[derive(Clone, Default)]
pub struct BaseVersion {
    pub levels: Vec<LevelState>,
}

#[allow(dead_code)]
impl BaseVersion {
    pub async fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for level in self.levels.iter().rev() {
            if let Some(value) = level.get(opts, key).await? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn scan(&self, _opts: &ReadOptions) -> BaseScanner {
        let children = self.levels.iter().map(|x| x.scan()).collect();
        BaseScanner::new(children)
    }
}

#[allow(dead_code)]
/// Scans all levels in a [`BaseVersion`].
pub struct BaseScanner {
    heap: BinaryHeap<OrderedLevelScanner>,
}

#[allow(dead_code)]
impl BaseScanner {
    pub fn new(children: Vec<LevelScanner>) -> Self {
        let children: Vec<OrderedLevelScanner> =
            children.into_iter().map(OrderedLevelScanner).collect();
        Self {
            heap: BinaryHeap::from(children),
        }
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        let mut children = std::mem::take(&mut self.heap).into_vec();
        for child in &mut children {
            child.seek_to_first().await?;
        }
        self.heap = BinaryHeap::from(children);
        Ok(())
    }

    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        let mut children = std::mem::take(&mut self.heap).into_vec();
        for child in &mut children {
            child.seek(target).await?;
        }
        self.heap = BinaryHeap::from(children);
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if let Some(mut s) = self.heap.peek_mut() {
            s.next().await?;
        }
        Ok(())
    }

    pub fn valid(&self) -> bool {
        if let Some(s) = self.heap.peek() {
            s.valid()
        } else {
            false
        }
    }

    pub fn key(&self) -> &[u8] {
        self.heap.peek().unwrap().key()
    }

    pub fn value(&self) -> &[u8] {
        self.heap.peek().unwrap().value()
    }
}

struct OrderedLevelScanner(LevelScanner);

impl OrderedLevelScanner {
    async fn seek_to_first(&mut self) -> Result<()> {
        self.0.seek_to_first().await
    }

    async fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.0.seek(target).await
    }

    async fn next(&mut self) -> Result<()> {
        self.0.next().await
    }

    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }
}

impl Ord for OrderedLevelScanner {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.valid() && other.valid() {
            other.key().cmp(self.key())
        } else if self.valid() {
            Ordering::Greater
        } else if other.valid() {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for OrderedLevelScanner {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrderedLevelScanner {
    fn eq(&self, other: &Self) -> bool {
        if self.valid() && other.valid() {
            self.key() == other.key()
        } else {
            !self.valid() && !self.valid()
        }
    }
}

impl Eq for OrderedLevelScanner {}
