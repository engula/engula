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

pub trait Batch {
    type Output;

    /// Returns the next `n` items.
    fn poll_next_batch(self: Pin<&mut Self>, cx: &mut Context<'_>, n: usize) -> Poll<Self::Output>;
}

macro_rules! impl_batch {
    () => {
        type Output = T::Output;

        fn poll_next_batch(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            len: usize,
        ) -> Poll<Self::Output> {
            Pin::new(&mut **self).poll_next_batch(cx, len)
        }
    };
}

impl<T: Batch + ?Sized + Unpin> Batch for Box<T> {
    impl_batch!();
}

impl<T: Batch + ?Sized + Unpin> Batch for &mut T {
    impl_batch!();
}

impl<T> Batch for Pin<T>
where
    T: DerefMut + Unpin,
    T::Target: Batch,
{
    type Output = <<T as Deref>::Target as Batch>::Output;

    fn poll_next_batch(self: Pin<&mut Self>, cx: &mut Context<'_>, n: usize) -> Poll<Self::Output> {
        self.get_mut().as_mut().poll_next_batch(cx, n)
    }
}
