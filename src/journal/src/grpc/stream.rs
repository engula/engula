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
use tonic::Streaming;

use super::{client::Client, proto::*};
use crate::{async_trait, Error, Event, Result, ResultStream, Timestamp};

#[derive(Clone)]
pub struct Stream {
    client: Client,
    stream: String,
}

impl Stream {
    pub fn new(client: Client, stream: String) -> Stream {
        Stream { client, stream }
    }

    async fn read_events_internal(&self, ts: Timestamp) -> Result<Streaming<ReadEventResponse>> {
        let input = ReadEventRequest {
            stream: self.stream.clone(),
            ts: serialize_ts(&ts)?,
        };
        self.client.read_event(input).await
    }
}

#[async_trait]
impl crate::Stream for Stream {
    async fn read_events(&self, ts: Timestamp) -> ResultStream<Vec<Event>> {
        let output = self.read_events_internal(ts).await;
        match output {
            Ok(output) => Box::new(output.map(|result| match result {
                Ok(resp) => {
                    let mut events = vec![];
                    for e in resp.events.iter().cloned() {
                        events.push(Event {
                            ts: deserialize_ts(&e.ts)?,
                            data: e.data,
                        })
                    }
                    Ok(events)
                }
                Err(status) => Err(Error::from(status)),
            })),
            Err(e) => Box::new(futures::stream::once(futures::future::err(e))),
        }
    }

    async fn append_event(&self, event: Event) -> Result<()> {
        let input = AppendEventRequest {
            stream: self.stream.clone(),
            ts: serialize_ts(&event.ts)?,
            data: event.data,
        };
        self.client.append_event(input).await?;
        Ok(())
    }

    async fn release_events(&self, ts: Timestamp) -> Result<()> {
        let input = ReleaseEventsRequest {
            stream: self.stream.clone(),
            ts: serialize_ts(&ts)?,
        };
        self.client.release_events(input).await?;
        Ok(())
    }
}
