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

use futures::{future, stream};
use pin_project::pin_project;
use tonic::Streaming;

use super::{
    client::Client,
    error::{Error, Result},
    proto::{
        deserialize_ts, serialize_ts, AppendEventRequest, ReadEventRequest, ReadEventResponse,
        ReleaseEventsRequest,
    },
};
use crate::{async_trait, Event, Stream, Timestamp};

#[derive(Clone)]
pub struct RemoteStream<T: Timestamp> {
    client: Client,
    stream: String,
    _t: PhantomData<T>,
}

impl<T: Timestamp> RemoteStream<T> {
    pub fn new(client: Client, stream: String) -> RemoteStream<T> {
        RemoteStream {
            client,
            stream,
            _t: PhantomData,
        }
    }

    async fn read_events_inner(&self, ts: T) -> Result<EventStream<T>> {
        let input = ReadEventRequest {
            stream: self.stream.clone(),
            ts: serialize_ts(&ts)?,
        };
        let output = self.client.read_event(input).await?;
        Ok(EventStream::new(output))
    }
}

#[async_trait]
impl<T: Timestamp> Stream for RemoteStream<T> {
    type Error = Error;
    type EventStream = EitherStream<T>;
    type Timestamp = T;

    async fn read_events(&self, ts: Self::Timestamp) -> Self::EventStream {
        match self.read_events_inner(ts).await {
            Ok(events) => EitherStream::Ok(events),
            Err(e) => EitherStream::Err(stream::once(future::err(e))),
        }
    }

    async fn append_event(&self, event: Event<Self::Timestamp>) -> Result<()> {
        let input = AppendEventRequest {
            stream: self.stream.clone(),
            ts: serialize_ts(&event.ts)?,
            data: event.data,
        };
        self.client.append_event(input).await?;
        Ok(())
    }

    async fn release_events(&self, ts: Self::Timestamp) -> Result<()> {
        let input = ReleaseEventsRequest {
            stream: self.stream.clone(),
            ts: serialize_ts(&ts)?,
        };
        self.client.release_events(input).await?;
        Ok(())
    }
}

type ErrorStream<T> = stream::Once<future::Ready<Result<Event<T>>>>;

#[pin_project(project = EitherStreamP)]
pub enum EitherStream<T: Timestamp> {
    Ok(#[pin] EventStream<T>),
    Err(#[pin] ErrorStream<T>),
}

impl<T: Timestamp> futures::Stream for EitherStream<T> {
    type Item = Result<Event<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EitherStreamP::Ok(stream) => stream.poll_next(cx),
            EitherStreamP::Err(stream) => stream.poll_next(cx),
        }
    }
}

pub struct EventStream<T: Timestamp> {
    events: Streaming<ReadEventResponse>,
    _t: PhantomData<T>,
}

impl<T: Timestamp> EventStream<T> {
    fn new(events: Streaming<ReadEventResponse>) -> Self {
        EventStream {
            events,
            _t: PhantomData,
        }
    }
}

impl<T: Timestamp> futures::Stream for EventStream<T> {
    type Item = Result<Event<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.events).poll_next(cx) {
            Poll::Ready(Some(Ok(resp))) => Poll::Ready(Some(Ok(Event {
                ts: deserialize_ts(&resp.ts)?,
                data: resp.data,
            }))),
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(s))) => Poll::Ready(Some(Err(Error::from(s)))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.events.size_hint()
    }
}
