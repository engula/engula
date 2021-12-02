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

use super::{client::Client, proto::*};
use crate::{
    async_trait,
    error::{Error, Result},
    Event, ResultStream, Timestamp,
};

#[derive(Clone)]
pub struct Stream {
    client: Client,
    stream: String,
}

impl Stream {
    pub fn new(client: Client, stream: String) -> Stream {
        Stream { client, stream }
    }
}

#[async_trait]
impl crate::Stream for Stream {
    async fn read_events(&self, ts: Timestamp) -> Result<ResultStream<Vec<Event>>> {
        let input = ReadEventRequest {
            stream: self.stream.clone(),
            ts: serialize_ts(&ts)?,
        };
        let output = self.client.read_event(input).await?;
        Ok(Box::new(output.map(|response| match response {
            Ok(resp) => {
                let events = resp
                    .events
                    .iter()
                    .cloned()
                    .map(|e| Event {
                        ts: deserialize_ts(&e.ts).unwrap(),
                        data: e.data,
                    })
                    .collect();
                Ok(events)
            }
            Err(status) => Err(Error::GrpcStatus(status)),
        })))
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
