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

use super::{batch_size::BatchSize, ResultStream};

pub trait ResultStreamExt: ResultStream {
    fn next(&mut self, batch_size: usize) -> NextFuture<'_, Self>
    where
        Self: Unpin,
    {
        NextFuture::new(self, batch_size)
    }

    fn batch_size(self, batch_size: usize) -> BatchSize<Self>
    where
        Self: Sized + Unpin,
        Self::Elem: Unpin,
    {
        BatchSize::new(self, batch_size)
    }
}

impl<T: ResultStream + ?Sized> ResultStreamExt for T {}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct NextFuture<'a, T: ?Sized> {
    inner: &'a mut T,
    batch_size: usize,
}

impl<T: ?Sized + Unpin> Unpin for NextFuture<'_, T> {}

impl<'a, T> NextFuture<'a, T>
where
    T: ResultStream + ?Sized + Unpin,
{
    fn new(inner: &'a mut T, batch_size: usize) -> Self {
        Self { inner, batch_size }
    }
}

impl<T> Future for NextFuture<'_, T>
where
    T: ResultStream + ?Sized + Unpin,
{
    type Output = Result<Vec<T::Elem>, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx, this.batch_size)
    }
}
