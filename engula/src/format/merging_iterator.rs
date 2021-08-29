use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::iterator::*;
use crate::common::Timestamp;

pub struct MergingIterator {
    heap: BinaryHeap<Reverse<Box<dyn Iterator>>>,
}

impl MergingIterator {
    fn new(children: Vec<Box<dyn Iterator>>) -> MergingIterator {
        let mut heap = BinaryHeap::new();
        for child in children {
            heap.push(Reverse(child));
        }
        MergingIterator { heap }
    }
}

impl Iterator for MergingIterator {
    fn valid(&self) -> bool {
        self.heap.peek().map_or(false, |x| x.0.valid())
    }

    fn seek_to_first(&mut self) {
        let mut children = Vec::new();
        for mut child in self.heap.drain() {
            child.0.seek_to_first();
            children.push(child);
        }
        self.heap = BinaryHeap::from(children);
    }

    fn seek(&mut self, ts: Timestamp, target: &[u8]) {
        let mut children = Vec::new();
        for mut child in self.heap.drain() {
            child.0.seek(ts, target);
            children.push(child);
        }
        self.heap = BinaryHeap::from(children);
    }

    fn next(&mut self) {
        if let Some(mut top) = self.heap.pop() {
            top.0.next();
            self.heap.push(top);
        }
    }

    fn current(&self) -> Option<Version> {
        self.heap.peek().and_then(|x| x.0.current())
    }
}
