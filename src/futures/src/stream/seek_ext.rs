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

use super::Seek;

pub trait SeekExt: Seek {
    fn seek<'a>(&'a mut self, target: &'a Self::Target) -> SeekFuture<'a, Self>
    where
        Self: Unpin,
        Self::Target: Unpin,
    {
        SeekFuture::new(self, target)
    }
}

impl<T: Seek + ?Sized> SeekExt for T {}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SeekFuture<'a, T: Seek + ?Sized> {
    inner: &'a mut T,
    target: &'a T::Target,
}

impl<T> Unpin for SeekFuture<'_, T>
where
    T: Seek + ?Sized + Unpin,
    T::Target: Unpin,
{
}

impl<'a, T> SeekFuture<'a, T>
where
    T: Seek + ?Sized + Unpin,
    T::Target: Unpin,
{
    fn new(inner: &'a mut T, target: &'a T::Target) -> Self {
        Self { inner, target }
    }
}

impl<T> Future for SeekFuture<'_, T>
where
    T: Seek + ?Sized + Unpin,
    T::Target: Unpin,
{
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_seek(cx, this.target)
    }
}
