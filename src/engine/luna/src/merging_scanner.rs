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

use crate::scan::Scan;

pub struct MergingScanner<S: Scan> {
    heap: BinaryHeap<OrderedScanner<S>>,
}

impl<S: Scan> MergingScanner<S> {
    pub fn new(children: Vec<S>) -> Self {
        let children: Vec<OrderedScanner<S>> = children.into_iter().map(OrderedScanner).collect();
        Self {
            heap: BinaryHeap::from(children),
        }
    }
}

impl<S: Scan> Scan for MergingScanner<S> {
    fn seek_to_first(&mut self) {
        let mut children = std::mem::take(&mut self.heap).into_vec();
        for child in &mut children {
            child.seek_to_first();
        }
        self.heap = BinaryHeap::from(children);
    }

    fn seek(&mut self, target: &[u8]) {
        let mut children = std::mem::take(&mut self.heap).into_vec();
        for child in &mut children {
            child.seek(target);
        }
        self.heap = BinaryHeap::from(children);
    }

    fn next(&mut self) {
        if let Some(mut s) = self.heap.peek_mut() {
            s.next();
        }
    }

    fn valid(&self) -> bool {
        if let Some(s) = self.heap.peek() {
            s.valid()
        } else {
            false
        }
    }

    fn key(&self) -> &[u8] {
        self.heap.peek().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.heap.peek().unwrap().value()
    }
}

struct OrderedScanner<S>(S);

impl<S: Scan> Scan for OrderedScanner<S> {
    fn seek_to_first(&mut self) {
        self.0.seek_to_first()
    }

    fn seek(&mut self, target: &[u8]) {
        self.0.seek(target)
    }

    fn next(&mut self) {
        self.0.next()
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

impl<S: Scan> Ord for OrderedScanner<S> {
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

impl<S: Scan> PartialOrd for OrderedScanner<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: Scan> PartialEq for OrderedScanner<S> {
    fn eq(&self, other: &Self) -> bool {
        if self.valid() && other.valid() {
            self.key() == other.key()
        } else {
            !self.valid() && !self.valid()
        }
    }
}

impl<S: Scan> Eq for OrderedScanner<S> {}
