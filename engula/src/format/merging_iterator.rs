use std::cmp::Reverse;
use std::collections::BinaryHeap;

use async_trait::async_trait;

use super::iterator::*;
use crate::common::Timestamp;
use crate::error::Error;

pub struct MergingIterator {
    heap: BinaryHeap<Reverse<Box<dyn Iterator>>>,
    error: Option<Error>,
}

impl MergingIterator {
    fn new(children: Vec<Box<dyn Iterator>>) -> MergingIterator {
        let mut heap = BinaryHeap::new();
        for child in children {
            heap.push(Reverse(child));
        }
        MergingIterator { heap, error: None }
    }
}

#[async_trait]
impl Iterator for MergingIterator {
    fn valid(&self) -> bool {
        self.error.is_none() && self.heap.peek().map_or(false, |x| x.0.valid())
    }

    fn error(&self) -> Option<Error> {
        self.error.clone()
    }

    async fn seek_to_first(&mut self) {
        assert!(self.error.is_none());
        let mut children = Vec::new();
        for mut child in self.heap.drain() {
            child.0.seek_to_first().await;
            if let Some(error) = child.0.error() {
                self.error = Some(error);
                break;
            }
            children.push(child);
        }
        self.heap = BinaryHeap::from(children);
    }

    async fn seek(&mut self, ts: Timestamp, target: &[u8]) {
        assert!(self.error.is_none());
        let mut children = Vec::new();
        for mut child in self.heap.drain() {
            child.0.seek(ts, target).await;
            if let Some(error) = child.0.error() {
                self.error = Some(error);
                return;
            }
            children.push(child);
        }
        self.heap = BinaryHeap::from(children);
    }

    async fn next(&mut self) {
        if let Some(mut top) = self.heap.pop() {
            top.0.next().await;
            if let Some(error) = top.0.error() {
                self.error = Some(error);
                return;
            }
            self.heap.push(top);
        }
    }

    fn current(&self) -> Option<Version> {
        if self.valid() {
            self.heap.peek().and_then(|x| x.0.current())
        } else {
            None
        }
    }
}
