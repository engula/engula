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

use std::{cmp::Reverse, collections::BinaryHeap};

use crate::scan::Scan;

pub struct MergingScanner<S: Scan + Ord> {
    heap: BinaryHeap<Reverse<S>>,
}

impl<S> MergingScanner<S>
where
    S: Scan + Ord,
{
    pub fn new(children: Vec<S>) -> Self {
        let children: Vec<Reverse<S>> = children.into_iter().map(Reverse).collect();
        Self {
            heap: BinaryHeap::from(children),
        }
    }
}

impl<S> Scan for MergingScanner<S>
where
    S: Scan + Ord,
{
    fn seek_to_first(&mut self) {
        let mut children = std::mem::take(&mut self.heap).into_vec();
        for child in &mut children {
            child.0.seek_to_first();
        }
        self.heap = BinaryHeap::from(children);
    }

    fn seek(&mut self, target: &[u8]) {
        let mut children = std::mem::take(&mut self.heap).into_vec();
        for child in &mut children {
            child.0.seek(target);
        }
        self.heap = BinaryHeap::from(children);
    }

    fn next(&mut self) {
        if let Some(mut iter) = self.heap.peek_mut() {
            iter.0.next();
        }
    }

    fn valid(&self) -> bool {
        if let Some(iter) = self.heap.peek() {
            iter.0.valid()
        } else {
            false
        }
    }

    fn key(&self) -> &[u8] {
        self.heap.peek().unwrap().0.key()
    }

    fn value(&self) -> &[u8] {
        self.heap.peek().unwrap().0.value()
    }
}
