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
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

pub trait Read {
    fn poll_read(
        self: Pin<&Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        pos: usize,
    ) -> Poll<io::Result<usize>>;
}

impl<T> Read for Pin<T>
where
    T: DerefMut + Unpin,
    T::Target: Read,
{
    fn poll_read(
        self: Pin<&Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        pos: usize,
    ) -> Poll<io::Result<usize>> {
        self.get_ref().as_ref().poll_read(cx, buf, pos)
    }
}

macro_rules! impl_read_for_ptrs {
    ($($t:ty),+) => {
        $(
        impl<T: Read + ?Sized + Unpin> Read for $t {
            fn poll_read(
                self: Pin<&Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
                pos: usize,
            ) -> Poll<io::Result<usize>> {
                Pin::new(&**self).poll_read(cx, buf, pos)
            }
        }
        )*
    };
}

impl_read_for_ptrs!(&mut T, Box<T>);

macro_rules! impl_read_for_slice {
    ($($t:ty),+) => {
        $(
        impl Read for $t {
            fn poll_read(
                self: Pin<&Self>,
                _: &mut Context<'_>,
                buf: &mut [u8],
                pos: usize,
            ) -> Poll<io::Result<usize>> {
                let len = if pos < self.len() {
                    let end = std::cmp::min(self.len(), pos + buf.len());
                    let src = &self[pos..end];
                    let dst = &mut buf[0..src.len()];
                    dst.copy_from_slice(src);
                    src.len()
                } else {
                    0
                };
                Poll::Ready(Ok(len))
            }
        }
    )*
    };
}

impl_read_for_slice!(&[u8], Vec<u8>);
