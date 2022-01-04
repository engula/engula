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

pub trait Seek {
    type Output;
    type Target;

    /// Seeks to a given target.
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target: &Self::Target,
    ) -> Poll<Self::Output>;
}

macro_rules! impl_seek {
    () => {
        type Output = T::Output;
        type Target = T::Target;

        fn poll_seek(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            target: &Self::Target,
        ) -> Poll<Self::Output> {
            Pin::new(&mut **self).poll_seek(cx, target)
        }
    };
}

impl<T: Seek + ?Sized + Unpin> Seek for Box<T> {
    impl_seek!();
}

impl<T: Seek + ?Sized + Unpin> Seek for &mut T {
    impl_seek!();
}

impl<T> Seek for Pin<T>
where
    T: DerefMut + Unpin,
    T::Target: Seek,
{
    type Output = <<T as Deref>::Target as Seek>::Output;
    type Target = <<T as Deref>::Target as Seek>::Target;

    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        target: &Self::Target,
    ) -> Poll<Self::Output> {
        self.get_mut().as_mut().poll_seek(cx, target)
    }
}
