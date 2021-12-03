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

use futures::StreamExt;
use tonic::{Request, Response, Status};

use super::{proto, proto::*};
use crate::{Event, Journal, Stream};

pub struct Server<J: Journal> {
    journal: J,
}

impl<J: Journal> Server<J> {
    pub fn new(journal: J) -> Self {
        Server { journal }
    }

    pub fn into_service(self) -> journal_server::JournalServer<Server<J>> {
        journal_server::JournalServer::new(self)
    }
}

#[tonic::async_trait]
impl<J: Journal> journal_server::Journal for Server<J> {
    type ReadEventsStream =
        Box<dyn futures::Stream<Item = Result<ReadEventsResponse, Status>> + Send + Unpin>;

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

    async fn read_events(
        &self,
        request: Request<ReadEventsRequest>,
    ) -> Result<Response<Self::ReadEventsStream>, Status> {
        let input = request.into_inner();
        let stream = self.journal.stream(&input.stream).await?;
        let event_stream = stream.read_events(deserialize_ts(&input.ts)?).await;
        Ok(Response::new(Box::new(event_stream.map(
            |result| match result {
                Ok(es) => {
                    let mut events = vec![];
                    for e in es.iter().cloned() {
                        events.push(proto::Event {
                            ts: serialize_ts(&e.ts)?,
                            data: e.data,
                        })
                    }
                    Ok(ReadEventsResponse { events })
                }
                Err(e) => Err(Status::from(e)),
            },
        ))))
    }
}
