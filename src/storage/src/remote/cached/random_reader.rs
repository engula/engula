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

use engula_futures::io::RandomRead;

pub struct RandomReader<B, C> {
    base: B,
    cache: Option<C>,
}

impl<B, C> RandomReader<B, C> {
    pub fn new(base: B, cache: Option<C>) -> Self {
        Self { base, cache }
    }
}

impl<B, C> RandomRead for RandomReader<B, C>
where
    B: RandomRead + Unpin,
    C: RandomRead + Unpin,
{
    fn poll_read(
        self: Pin<&Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        pos: usize,
    ) -> Poll<io::Result<usize>> {
        let this = self.get_ref();
        if let Some(r) = this.cache.as_ref() {
            Pin::new(r).poll_read(cx, buf, pos)
        } else {
            Pin::new(&this.base).poll_read(cx, buf, pos)
        }
    }
}
