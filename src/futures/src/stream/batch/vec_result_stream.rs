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
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use super::ResultStream;

pub struct VecResultStream<T, E> {
    elems: Vec<T>,
    error: PhantomData<E>,
}

impl<T, E> VecResultStream<T, E> {
    pub fn new(elems: Vec<T>) -> Self {
        Self {
            elems,
            error: PhantomData,
        }
    }
}

impl<T, E> ResultStream for VecResultStream<T, E>
where
    T: Unpin,
    E: Unpin,
{
    type Elem = T;
    type Error = E;

    fn poll_next(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        batch_size: usize,
    ) -> Poll<Result<Vec<Self::Elem>, Self::Error>> {
        let this = self.get_mut();
        let mut batch = std::mem::take(&mut this.elems);
        if batch_size < batch.len() {
            this.elems = batch.split_off(batch_size);
        }
        Poll::Ready(Ok(batch))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.elems.len(), Some(self.elems.len()))
    }
}
