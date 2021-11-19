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

mod client;
mod error;
mod journal;
mod proto;
mod server;
mod stream;

pub use self::{
    client::Client,
    error::{Error, Result},
    journal::RemoteJournal,
    server::Server,
    stream::{EventStream, RemoteStream},
};

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let j: mem::MemJournal<u64> = mem::MemJournal::default();
            let server = Server::new(j);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let url = format!("http://{}", local_addr);
        let journal = RemoteJournal::connect(&url).await?;
        let stream = journal.create_stream("s").await?;
        let event = Event {
            ts: 1,
            data: vec![0, 1, 2],
        };
        stream.append_event(event.clone()).await?;
        {
            let mut events = stream.read_events(0).await;
            let got = events.next().await.unwrap()?;
            assert_eq!(got, event);
        }
        stream.release_events(2).await?;
        {
            let mut events = stream.read_events(0).await;
            let got = events.next().await;
            assert!(got.is_none());
        }
        let _ = journal.delete_stream("s").await?;
        Ok(())
    }
}
