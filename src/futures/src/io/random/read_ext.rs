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
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, AsyncRead, Future};

use super::Read;

pub trait ReadExt: Read {
    fn read<'a>(&'a self, buf: &'a mut [u8], pos: usize) -> ReadFuture<'a, Self>
    where
        Self: Unpin,
    {
        ReadFuture::new(self, buf, pos)
    }

    fn read_exact<'a>(&'a self, buf: &'a mut [u8], pos: usize) -> ReadExactFuture<'a, Self>
    where
        Self: Unpin,
    {
        ReadExactFuture::new(self, buf, pos)
    }
}

impl<R: Read + ?Sized> ReadExt for R {}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadFuture<'a, T: ?Sized> {
    inner: &'a T,
    buf: &'a mut [u8],
    pos: usize,
}

impl<T: ?Sized + Unpin> Unpin for ReadFuture<'_, T> {}

impl<'a, T: Read + ?Sized + Unpin> ReadFuture<'a, T> {
    pub(super) fn new(inner: &'a T, buf: &'a mut [u8], pos: usize) -> Self {
        Self { inner, buf, pos }
    }
}

impl<T: Read + ?Sized + Unpin> Future for ReadFuture<'_, T> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(this.inner).poll_read(cx, this.buf, this.pos)
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadExactFuture<'a, T: ?Sized> {
    inner: &'a T,
    buf: &'a mut [u8],
    pos: usize,
}

impl<T: ?Sized + Unpin> Unpin for ReadExactFuture<'_, T> {}

impl<'a, T> ReadExactFuture<'a, T>
where
    T: Read + ?Sized + Unpin,
{
    pub(super) fn new(inner: &'a T, buf: &'a mut [u8], pos: usize) -> Self {
        Self { inner, buf, pos }
    }
}

impl<T> Future for ReadExactFuture<'_, T>
where
    T: Read + ?Sized + Unpin,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        while !this.buf.is_empty() {
            let n = ready!(Pin::new(this.inner).poll_read(cx, this.buf, this.pos))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
            }
            let (_, rest) = std::mem::take(&mut this.buf).split_at_mut(n);
            this.buf = rest;
            this.pos += n;
        }
        Poll::Ready(Ok(()))
    }
}

pub trait ReadFromPosExt: Read {
    fn to_async_read(self, pos: usize) -> ReadFromPos<Self>
    where
        Self: Unpin + Sized,
    {
        ReadFromPos::new(self, pos)
    }
}

impl<R: Read> ReadFromPosExt for R {}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadFromPos<T>
where
    T: Read + Unpin + Sized,
{
    inner: T,
    pos: usize,
}

impl<T> ReadFromPos<T>
where
    T: Read + Unpin + Sized,
{
    pub(super) fn new(inner: T, pos: usize) -> Self {
        Self { inner, pos }
    }
}

impl<T> AsyncRead for ReadFromPos<T>
where
    T: Read + Unpin + Sized,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let size = ready!(Pin::new(&this.inner).poll_read(cx, buf, this.pos))?;
        this.pos += size;
        Poll::Ready(Ok(size))
    }
}

impl<T> tokio::io::AsyncRead for ReadFromPos<T>
where
    T: Read + Unpin + Sized,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let size =
            ready!(Pin::new(&this.inner).poll_read(cx, buf.initialize_unfilled(), this.pos))?;
        buf.set_filled(buf.filled().len() + size);
        this.pos += size;
        Poll::Ready(Ok(()))
    }
}
