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

use tonic::{Request, Response, Status};

use super::proto::*;
use crate::{Event, Journal, Stream, Timestamp};

pub struct Server<S, J, T>
where
    S: Stream,
    J: Journal<S>,
    T: Timestamp,
{
    journal: J,
    _stream: PhantomData<S>,
    _t: PhantomData<T>,
}

impl<S, J, T> Server<S, J, T>
where
    T: Timestamp + 'static,
    S: Stream<Timestamp = T> + Send + Sync + 'static,
    S::Error: Send + Sync + 'static,
    S::EventStream: Send + Sync + 'static,
    J: Journal<S> + Send + Sync + 'static,
    Status: From<S::Error>,
{
    pub fn new(journal: J) -> Self {
        Server {
            journal,
            _stream: PhantomData,
            _t: PhantomData,
        }
    }

    pub fn into_service(self) -> journal_server::JournalServer<Server<S, J, T>> {
        journal_server::JournalServer::new(self)
    }
}

#[tonic::async_trait]
impl<S, J, T> journal_server::Journal for Server<S, J, T>
where
    T: Timestamp + 'static,
    S: Stream<Timestamp = T> + Send + Sync + 'static,
    S::Error: Send + Sync + 'static,
    S::EventStream: Send + Sync + 'static,
    J: Journal<S> + Send + Sync + 'static,
    Status: From<S::Error>,
{
    type ReadEventStream = EventStream<S>;

    async fn create_stream(
        &self,
        request: Request<CreateStreamRequest>,
    ) -> Result<Response<CreateStreamResponse>, Status> {
        let input = request.into_inner();
        self.journal.create_stream(&input.stream).await?;
        Ok(Response::new(CreateStreamResponse {}))
    }

    async fn delete_stream(
        &self,
        request: Request<DeleteStreamRequest>,
    ) -> Result<Response<DeleteStreamResponse>, Status> {
        let input = request.into_inner();
        self.journal.delete_stream(&input.stream).await?;
        Ok(Response::new(DeleteStreamResponse {}))
    }

    async fn append_event(
        &self,
        request: Request<AppendEventRequest>,
    ) -> Result<Response<AppendEventResponse>, Status> {
        let input = request.into_inner();
        let stream = self.journal.stream(&input.stream).await?;
        stream
            .append_event(Event {
                ts: deserialize_ts(&input.ts)?,
                data: input.data,
            })
            .await?;
        Ok(Response::new(AppendEventResponse {}))
    }

    async fn release_events(
        &self,
        request: Request<ReleaseEventsRequest>,
    ) -> Result<Response<ReleaseEventsResponse>, Status> {
        let input = request.into_inner();
        let stream = self.journal.stream(&input.stream).await?;
        stream.release_events(deserialize_ts(&input.ts)?).await?;
        Ok(Response::new(ReleaseEventsResponse {}))
    }

    async fn read_event(
        &self,
        request: Request<ReadEventRequest>,
    ) -> Result<Response<Self::ReadEventStream>, Status> {
        let input = request.into_inner();
        let stream = self.journal.stream(&input.stream).await?;
        let events = stream.read_events(deserialize_ts(&input.ts)?).await;
        Ok(Response::new(EventStream::new(events)))
    }
}

pub struct EventStream<S>
where
    S: Stream,
{
    events: S::EventStream,
}

impl<S> EventStream<S>
where
    S: Stream,
{
    fn new(events: S::EventStream) -> Self {
        EventStream { events }
    }
}

impl<S> futures::Stream for EventStream<S>
where
    S: Stream,
    Status: From<S::Error>,
{
    type Item = Result<ReadEventResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.events).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(ReadEventResponse {
                ts: serialize_ts(&event.ts)?,
                data: event.data,
            }))),
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(Status::from(err)))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.events.size_hint()
    }
}
