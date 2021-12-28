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
    collections::BinaryHeap,
    pin::Pin,
    task::{Context, Poll},
};

use futures::ready;

use super::iter::Iter;
use crate::Result;

pub struct MergingIter<I> {
    heap: BinaryHeap<I>,
    buffer: Vec<I>,
}

#[allow(dead_code)]
impl<I> MergingIter<I>
where
    I: Iter + Ord + Unpin,
{
    pub fn new(children: Vec<I>) -> Self {
        Self {
            heap: BinaryHeap::new(),
            buffer: children,
        }
    }

    fn take_children(&mut self) -> Vec<I> {
        let heap = std::mem::take(&mut self.heap);
        let mut heap = heap.into_vec();
        self.buffer.append(&mut heap);
        std::mem::take(&mut self.buffer)
    }
}

impl<I> Iter for MergingIter<I>
where
    I: Iter + Ord + Unpin,
{
    fn poll_seek_to_first(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let children = self.take_children();
        let mut heap = Vec::with_capacity(children.len());
        for mut iter in children {
            ready!(Pin::new(&mut iter).poll_seek_to_first(cx))?;
            if iter.valid() {
                heap.push(iter);
            } else {
                self.buffer.push(iter);
            }
        }
        self.heap = BinaryHeap::from(heap);
        Poll::Ready(Ok(()))
    }

    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target: &[u8],
    ) -> Poll<Result<()>> {
        let children = self.take_children();
        let mut heap = Vec::with_capacity(children.len());
        for mut iter in children {
            ready!(Pin::new(&mut iter).poll_seek(cx, target))?;
            if iter.valid() {
                heap.push(iter);
            } else {
                self.buffer.push(iter);
            }
        }
        self.heap = BinaryHeap::from(heap);
        Poll::Ready(Ok(()))
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(mut iter) = self.heap.pop() {
            ready!(Pin::new(&mut iter).poll_next(cx))?;
            if iter.valid() {
                self.heap.push(iter);
            } else {
                self.buffer.push(iter);
            }
        }
        Poll::Ready(Ok(()))
    }

    fn valid(&self) -> bool {
        if let Some(top) = self.heap.peek() {
            top.valid()
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
