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

mod mem;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request, Response, Status, Streaming};

use super::{proto::server as serverpb, Entry};

#[derive(Debug)]
struct Server {
    store: Arc<Mutex<mem::Store>>,
}

impl Server {
    #[allow(dead_code)]
    pub fn into_service(self) -> serverpb::shared_journal_server::SharedJournalServer<Server> {
        serverpb::shared_journal_server::SharedJournalServer::new(self)
    }
}

#[async_trait]
#[allow(unused)]
impl serverpb::shared_journal_server::SharedJournal for Server {
    type ReadStream = mem::ReplicaReader;

    async fn store(
        &self,
        input: Request<serverpb::StoreRequest>,
    ) -> Result<Response<serverpb::StoreResponse>, Status> {
        let req = input.into_inner();
        let mut store = self.store.lock().await;
        store.store(
            req.stream_id,
            req.seg_epoch,
            req.acked_seq,
            req.first_index,
            req.events
                .into_iter()
                .map(|e| Entry::Event {
                    epoch: req.epoch,
                    event: e.into(),
                })
                .collect(),
        )?;
        Ok(Response::new(serverpb::StoreResponse {}))
    }

    async fn read(
        &self,
        input: Request<serverpb::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = input.into_inner();
        let mut store = self.store.lock().await;
        let stream = store.read(
            req.stream_id,
            req.seg_epoch,
            req.start_index,
            req.limit as usize,
        )?;
        Ok(Response::new(stream))
    }
}

type SharedJournalClient = serverpb::shared_journal_client::SharedJournalClient<Channel>;

#[derive(Clone)]
#[allow(unused)]
pub struct Client {
    client: SharedJournalClient,
}

#[allow(dead_code)]
impl Client {
    pub async fn connect(addr: &str) -> crate::Result<Client> {
        let addr = format!("http://{}", addr);
        let client = SharedJournalClient::connect(addr).await?;
        Ok(Client { client })
    }

    async fn store(&self, input: serverpb::StoreRequest) -> crate::Result<serverpb::StoreResponse> {
        let mut client = self.client.clone();
        let resp = client.store(input).await?;
        Ok(resp.into_inner())
    }

    async fn read(
        &self,
        input: serverpb::ReadRequest,
    ) -> crate::Result<Streaming<serverpb::Entry>> {
        let mut client = self.client.clone();
        let resp = client.read(input).await?;
        Ok(resp.into_inner())
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::{remote::shared::proto::server::ReadRequest, Sequence};

    fn encode(epoch: u32, index: u32) -> Sequence {
        ((epoch as u64) << 32) | (index as u64)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn basic_store_and_read() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let stores = vec![
            serverpb::StoreRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: 0,
                first_index: 0,
                events: vec![vec![0u8], vec![2u8], vec![4u8]],
            },
            serverpb::StoreRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: encode(1, 2),
                first_index: 3,
                events: vec![vec![6u8], vec![8u8]],
            },
            serverpb::StoreRequest {
                stream_id: 1,
                seg_epoch: 1,
                epoch: 1,
                acked_seq: encode(1, 4),
                first_index: 5,
                events: vec![],
            },
        ];

        let entries = vec![
            Entry::Event {
                epoch: 1,
                event: vec![0u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![2u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![4u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![6u8].into(),
            },
            Entry::Event {
                epoch: 1,
                event: vec![8u8].into(),
            },
        ];

        struct Test<'a> {
            from: u32,
            limit: u32,
            expect: &'a [Entry],
        }

        let tests = vec![
            Test {
                from: 0,
                limit: 1,
                expect: &entries[0..1],
            },
            Test {
                from: 3,
                limit: 2,
                expect: &entries[3..],
            },
            Test {
                from: 0,
                limit: 5,
                expect: &entries[..],
            },
        ];

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = Server {
                store: Arc::new(Mutex::new(mem::Store::new())),
            };
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let client = Client::connect(&local_addr.to_string()).await?;
        for store in stores {
            client.store(store).await?;
        }

        for test in tests {
            let req = ReadRequest {
                stream_id: 1,
                seg_epoch: 1,
                start_index: test.from,
                limit: test.limit,
            };
            let mut stream = client.read(req).await?;
            let mut got = Vec::<Entry>::new();
            while let Some(item) = stream.next().await {
                got.push(item?.into());
            }
            assert_eq!(got.len(), test.expect.len());
            assert!(got.iter().zip(test.expect.iter()).all(|(l, r)| l == r));
        }
        Ok(())
    }
}
