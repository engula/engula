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
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use crate::Result;

pub type BoxIter = Pin<Box<dyn Iter>>;

pub trait Iter {
    fn poll_seek_to_first(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>;

    fn poll_seek(self: Pin<&mut Self>, cx: &mut Context<'_>, target: &[u8]) -> Poll<Result<()>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>;

    fn current(&self) -> Option<(&[u8], &[u8])>;
}

impl<I: ?Sized + Iter + Unpin> Iter for Box<I> {
    fn poll_seek_to_first(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut **self).poll_seek_to_first(cx)
    }

    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target: &[u8],
    ) -> Poll<Result<()>> {
        Pin::new(&mut **self).poll_seek(cx, target)
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut **self).poll_next(cx)
    }

    fn current(&self) -> Option<(&[u8], &[u8])> {
        (**self).current()
    }
}

impl<P> Iter for Pin<P>
where
    P: DerefMut + Unpin,
    P::Target: Iter,
{
    fn poll_seek_to_first(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.get_mut().as_mut().poll_seek_to_first(cx)
    }

    fn poll_seek(self: Pin<&mut Self>, cx: &mut Context<'_>, target: &[u8]) -> Poll<Result<()>> {
        self.get_mut().as_mut().poll_seek(cx, target)
    }

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.get_mut().as_mut().poll_next(cx)
    }

    fn current(&self) -> Option<(&[u8], &[u8])> {
        (**self).current()
    }
}
