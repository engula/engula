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
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

/// An extended stream that can return a batch of items at once.
pub trait BatchStream: Stream {
    type Batch;

    /// Returns the next `n` items.
    fn poll_next_batch(self: Pin<&mut Self>, cx: &mut Context<'_>, n: usize) -> Poll<Self::Batch>;
}

macro_rules! impl_batch_stream {
    () => {
        type Batch = T::Batch;

        fn poll_next_batch(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            len: usize,
        ) -> Poll<Self::Batch> {
            Pin::new(&mut **self).poll_next_batch(cx, len)
        }
    };
}

impl<T: BatchStream + ?Sized + Unpin> BatchStream for Box<T> {
    impl_batch_stream!();
}

impl<T: BatchStream + ?Sized + Unpin> BatchStream for &mut T {
    impl_batch_stream!();
}

impl<T> BatchStream for Pin<T>
where
    T: DerefMut + Unpin,
    T::Target: BatchStream,
{
    type Batch = <<T as Deref>::Target as BatchStream>::Batch;

    fn poll_next_batch(self: Pin<&mut Self>, cx: &mut Context<'_>, n: usize) -> Poll<Self::Batch> {
        self.get_mut().as_mut().poll_next_batch(cx, n)
    }
}
