// Copyright 2022 The Engula Authors.
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
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use stream_engine_proto::ReadResponse;

use crate::{Entry, TonicResult};

pub struct TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<ReadResponse>>,
{
    stream: &'a mut S,
    terminated: Option<TonicResult<()>>,
    entries: Vec<(u32, Entry)>,
}

impl<'a, S> TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<ReadResponse>>,
{
    pub fn new(stream: &'a mut S) -> Self {
        TryBatchNext {
            stream,
            terminated: None,
            entries: Vec::default(),
        }
    }
}

impl<'a, S> Stream for TryBatchNext<'a, S>
where
    S: Stream<Item = TonicResult<ReadResponse>> + Unpin,
{
    type Item = TonicResult<Vec<(u32, Entry)>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while this.terminated.is_none() {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(resp)) => match resp {
                    Ok(resp) => this.entries.push((resp.index, resp.entry.unwrap().into())),
                    Err(status) => {
                        this.terminated = Some(Err(status));
                    }
                },
                Poll::Ready(None) => {
                    this.terminated = Some(Ok(()));
                }
                Poll::Pending => {
                    if this.entries.is_empty() {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Some(Ok(std::mem::take(&mut this.entries))));
                    }
                }
            }
        }

        if !this.entries.is_empty() {
            return Poll::Ready(Some(Ok(std::mem::take(&mut this.entries))));
        }

        match std::mem::replace(&mut this.terminated, Some(Ok(()))) {
            Some(Ok(())) => Poll::Ready(None),
            Some(Err(status)) => Poll::Ready(Some(Err(status))),
            None => unreachable!(),
        }
    }
}
