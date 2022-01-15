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

/// A stream that yields a batch of elements at a time.
pub trait ResultStream {
    type Elem;
    type Error;

    /// Returns the next `batch_size` elements.
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch_size: usize,
    ) -> Poll<Result<Vec<Self::Elem>, Self::Error>>;

    /// Returns the bounds on the remaining length of the stream.
    ///
    /// The returned tuple represents the lower bound (first element) and the
    /// upper bound (second element) of the length. If the second element is
    /// [`None`], it means that the upper bound is unknown.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

macro_rules! impl_stream {
    () => {
        type Elem = T::Elem;
        type Error = T::Error;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            batch_size: usize,
        ) -> Poll<Result<Vec<Self::Elem>, Self::Error>> {
            Pin::new(&mut **self).poll_next(cx, batch_size)
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (**self).size_hint()
        }
    };
}

impl<T: ResultStream + ?Sized + Unpin> ResultStream for Box<T> {
    impl_stream!();
}

impl<T: ResultStream + ?Sized + Unpin> ResultStream for &mut T {
    impl_stream!();
}

impl<T> ResultStream for Pin<T>
where
    T: DerefMut + Unpin,
    T::Target: ResultStream,
{
    type Elem = <<T as Deref>::Target as ResultStream>::Elem;
    type Error = <<T as Deref>::Target as ResultStream>::Error;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch_size: usize,
    ) -> Poll<Result<Vec<Self::Elem>, Self::Error>> {
        self.get_mut().as_mut().poll_next(cx, batch_size)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.as_ref().get_ref().size_hint()
    }
}
