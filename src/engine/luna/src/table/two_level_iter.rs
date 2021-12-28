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
    pin::Pin,
    task::{Context, Poll},
};

use futures::ready;

use super::iter::Iter;
use crate::Result;

pub trait IterGen {
    type Iter: Iter + Unpin;

    fn poll_gen(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        value: &[u8],
    ) -> Poll<Result<Self::Iter>>;
}

pub struct TwoLevelIter<I: Iter, G: IterGen> {
    first_iter: I,
    second_iter: Option<G::Iter>,
    second_iter_gen: G,
}

impl<I: Iter + Unpin, G: IterGen + Unpin> TwoLevelIter<I, G> {
    fn borrow_fields(&mut self) -> (&I, &mut G) {
        (&self.first_iter, &mut self.second_iter_gen)
    }

    fn init_second_iter(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let (first_iter, second_iter_gen) = self.borrow_fields();
        if first_iter.valid() {
            let value = first_iter.value();
            let second_iter = ready!(Pin::new(second_iter_gen).poll_gen(cx, value))?;
            self.second_iter = Some(second_iter);
        } else {
            self.second_iter = None;
        }
        Poll::Ready(Ok(()))
    }

    fn skip_until_valid(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        while let Some(iter) = self.second_iter.as_ref() {
            if iter.valid() {
                break;
            }
            ready!(Pin::new(&mut self.first_iter).poll_next(cx))?;
            ready!(self.as_mut().init_second_iter(cx))?;
        }
        Poll::Ready(Ok(()))
    }
}

impl<I, G> Iter for TwoLevelIter<I, G>
where
    I: Iter + Unpin,
    G: IterGen + Unpin,
{
    fn poll_seek_to_first(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(Pin::new(&mut self.first_iter).poll_seek_to_first(cx))?;
        ready!(self.as_mut().init_second_iter(cx))?;
        if let Some(iter) = self.second_iter.as_mut() {
            ready!(Pin::new(iter).poll_seek_to_first(cx))?;
            ready!(self.skip_until_valid(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target: &[u8],
    ) -> Poll<Result<()>> {
        ready!(Pin::new(&mut self.first_iter).poll_seek(cx, target))?;
        ready!(self.as_mut().init_second_iter(cx))?;
        if let Some(iter) = self.second_iter.as_mut() {
            ready!(Pin::new(iter).poll_seek(cx, target))?;
            ready!(self.skip_until_valid(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(iter) = self.second_iter.as_mut() {
            ready!(Pin::new(iter).poll_next(cx))?;
            ready!(self.skip_until_valid(cx))?;
        }
        Poll::Ready(Ok(()))
    }

    fn valid(&self) -> bool {
        if let Some(iter) = self.second_iter.as_ref() {
            iter.valid()
        } else {
            false
        }
    }

    fn key(&self) -> &[u8] {
        self.second_iter.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.second_iter.as_ref().unwrap().value()
    }
}
