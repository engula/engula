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

use std::{
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    collections::BinaryHeap,
    pin::Pin,
    task::{Context, Poll},
};

use futures::ready;

use super::iter::{BoxIter, Iter};
use crate::Result;

pub struct MergingIter {
    children: Vec<BoxIter>,
    heap: BinaryHeap<BoxIter>,
}

#[allow(dead_code)]
impl MergingIter {
    pub fn new(children: Vec<BoxIter>) -> Self {
        Self {
            children,
            heap: BinaryHeap::new(),
        }
    }

    fn take_children(&mut self) -> Vec<BoxIter> {
        if !self.children.is_empty() {
            std::mem::take(&mut self.children)
        } else {
            let heap = std::mem::take(&mut self.heap);
            heap.into_vec()
        }
    }
}

impl Iter for MergingIter {
    fn poll_seek_to_first(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut children = self.take_children();
        for child in &mut children {
            ready!(Pin::new(child).poll_seek_to_first(cx))?;
        }
        self.heap = BinaryHeap::from(children);
        Poll::Ready(Ok(()))
    }

    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target: &[u8],
    ) -> Poll<Result<()>> {
        let mut children = self.take_children();
        for child in &mut children {
            ready!(Pin::new(child).poll_seek(cx, target))?;
        }
        self.heap = BinaryHeap::from(children);
        Poll::Ready(Ok(()))
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(mut first) = self.heap.pop() {
            ready!(Pin::new(&mut first).poll_next(cx))?;
            self.heap.push(first);
        }
        Poll::Ready(Ok(()))
    }

    fn current(&self) -> Option<(&[u8], &[u8])> {
        self.heap.peek().and_then(|x| x.current())
    }
}

impl Eq for dyn Iter {}

impl PartialEq for dyn Iter {
    fn eq(&self, other: &Self) -> bool {
        match (self.current(), other.current()) {
            (Some(a), Some(b)) => a.0 == b.0,
            (None, None) => true,
            _ => false,
        }
    }
}

impl Ord for dyn Iter {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
            (Some(a), Some(b)) => b.0.cmp(a.0),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }
}

impl PartialOrd for dyn Iter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
