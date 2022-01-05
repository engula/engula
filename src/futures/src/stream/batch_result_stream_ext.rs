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
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::ready;

use super::BatchResultStream;

pub trait BatchResultStreamExt: BatchResultStream {
    fn next(&mut self, batch_size: usize) -> NextFuture<'_, Self>
    where
        Self: Unpin,
        Self::Elem: Unpin,
    {
        NextFuture::new(self, batch_size)
    }

    fn collect(&mut self, batch_size: usize) -> CollectFuture<'_, Self>
    where
        Self: Unpin,
        Self::Elem: Unpin,
    {
        CollectFuture::new(self, batch_size)
    }
}

impl<T: BatchResultStream + ?Sized> BatchResultStreamExt for T {}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct NextFuture<'a, T: ?Sized> {
    inner: &'a mut T,
    batch_size: usize,
}

impl<T: ?Sized + Unpin> Unpin for NextFuture<'_, T> {}

impl<'a, T> NextFuture<'a, T>
where
    T: BatchResultStream + ?Sized + Unpin,
{
    fn new(inner: &'a mut T, batch_size: usize) -> Self {
        Self { inner, batch_size }
    }
}

impl<T> Future for NextFuture<'_, T>
where
    T: BatchResultStream + ?Sized + Unpin,
{
    type Output = Result<Vec<T::Elem>, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx, this.batch_size)
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct CollectFuture<'a, T: BatchResultStream + ?Sized> {
    inner: &'a mut T,
    batch_size: usize,
    collection: Vec<T::Elem>,
}

impl<T: BatchResultStream + ?Sized + Unpin> Unpin for CollectFuture<'_, T> {}

impl<'a, T> CollectFuture<'a, T>
where
    T: BatchResultStream + ?Sized + Unpin,
{
    fn new(inner: &'a mut T, batch_size: usize) -> Self {
        let cap = match inner.size_hint() {
            (_, Some(max)) => max,
            (min, None) => min,
        };
        Self {
            inner,
            batch_size,
            collection: Vec::with_capacity(cap),
        }
    }
}

impl<T> Future for CollectFuture<'_, T>
where
    T: BatchResultStream + ?Sized + Unpin,
{
    type Output = Result<Vec<T::Elem>, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut inner = Pin::new(&mut this.inner);
        loop {
            let batch = ready!(inner.as_mut().poll_next(cx, this.batch_size))?;
            if batch.is_empty() {
                break;
            }
            this.collection.extend(batch);
        }
        Poll::Ready(Ok(std::mem::take(&mut this.collection)))
    }
}
