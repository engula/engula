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
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use crate::{iterator::LevelIter, Key, Result, Timestamp};

pub struct MergingIterator {
    pub levels: Vec<LevelIter>,
    pub iter_heap: BinaryHeap<Reverse<HeapItem>>,

    pub init: bool,
    pub snapshot: Option<Timestamp>,
}

impl MergingIterator {
    pub fn new(levels: Vec<LevelIter>, snapshot: Option<u64>) -> Self {
        let iter_heap = BinaryHeap::with_capacity(levels.len());
        Self {
            levels,
            iter_heap,
            init: false,
            snapshot,
        }
    }

    pub fn key(&self) -> Key<'_> {
        debug_assert!(self.valid());
        let (k, _) = self.current_entry().unwrap();
        k
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        let (_, v) = self.current_entry().unwrap();
        v
    }

    pub fn valid(&self) -> bool {
        if !self.init {
            return true;
        }
        if let Some(iter) = self.iter_heap.peek() {
            return self.levels[iter.0.index].valid();
        }
        false
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        for l in self.levels.iter_mut() {
            l.seek_to_first().await?;
        }
        self.init_heap();
        if !self.init {
            self.init = true
        }
        self.seek_to_next_visible().await?;
        Ok(())
    }

    pub async fn seek(&mut self, target: Key<'_>) -> Result<()> {
        for l in self.levels.iter_mut() {
            l.seek(target).await?;
        }
        self.init_heap();
        if !self.init {
            self.init = true
        }
        self.seek_to_next_visible().await?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if !self.init {
            self.seek_to_first().await?;
            return Ok(());
        }

        self.next_entry().await?;
        self.seek_to_next_visible().await?;
        Ok(())
    }

    fn init_heap(&mut self) {
        self.iter_heap.clear();
        for (index, iter) in self.levels.iter().enumerate() {
            if !iter.valid() {
                continue;
            }
            if let Some(key) = iter.key() {
                self.iter_heap.push(Reverse(HeapItem {
                    index,
                    key: key.to_owned(),
                }));
            }
        }
    }

    async fn next_entry(&mut self) -> Result<()> {
        let prev_smallest = self.iter_heap.peek();
        if prev_smallest.is_none() {
            return Ok(());
        }

        let prev_level_iter = &mut self.levels[prev_smallest.unwrap().0.index];
        prev_level_iter.next().await?;

        if prev_level_iter.valid() {
            if !prev_level_iter.valid() {
                print!("")
            }
            self.iter_heap.peek_mut().unwrap().0.key = prev_level_iter.key().unwrap().to_owned();
            let item = self.iter_heap.pop().unwrap();
            self.iter_heap.push(item)
        } else {
            self.iter_heap.pop();
        }
        Ok(())
    }

    async fn seek_to_next_visible(&mut self) -> Result<()> {
        while !self.iter_heap.is_empty() {
            let level = &self.iter_heap.peek().unwrap().0;
            let key = Key::from(level.key.as_slice());

            if self.check_key_visible(key) {
                return Ok(());
            }
            self.next_entry().await?;
        }
        Ok(())
    }

    fn check_key_visible(&self, key: Key<'_>) -> bool {
        if let Some(snapshot) = self.snapshot {
            return key.ts() <= snapshot;
        }
        true
    }

    fn current_entry(&self) -> Option<(Key<'_>, &[u8])> {
        if self.iter_heap.is_empty() {
            return None;
        }
        if let Some(item) = self.iter_heap.peek() {
            let l = &self.levels[item.0.index];
            let key = l.key()?;
            return Some((key, l.value()));
        }
        None
    }
}

#[derive(Eq)]
pub struct HeapItem {
    index: usize,
    key: Vec<u8>,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        Key::from(self.key.as_slice()).cmp(&Key::from(other.key.as_slice()))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
