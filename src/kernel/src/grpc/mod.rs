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
mod kernel;
mod proto;
mod server;

pub use self::{client::Client, kernel::Kernel, server::Server};

#[cfg(test)]
mod tests {
    use engula_journal::{grpc::Server as JournalServer, mem::Journal as MemJournal, Journal};
    use engula_storage::{grpc::Server as StorageServer, mem::Storage as MemStorage, Storage};
    use futures::TryStreamExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_stream::wrappers::TcpListenerStream;

    use super::Server;
    use crate::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let j = MemJournal::default();
            let stream = j.create_stream("DEFAULT").await.unwrap();
            let s = MemStorage::default();
            let bucket = s.create_bucket("DEFAULT").await.unwrap();
            let manifest = mem::Manifest::default();
            let kernel = mem::Kernel::init(stream, bucket, manifest).await.unwrap();
            let server = Server::new(kernel);
            let journal_server = JournalServer::new(j);
            let storage_server = StorageServer::new(s);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .add_service(journal_server.into_service())
                .add_service(storage_server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let url = format!("http://{}", local_addr);
        let kernel = grpc::Kernel::connect(&url).await?;

        // For stream
        let stream = kernel.stream().await?;
        let ts = 31340128116183;
        let event = Event {
            ts: ts.into(),
            data: vec![0, 1, 2],
        };
        stream.append_event(event.clone()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![event]);
        }
        stream.release_events((ts + 1).into()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![]);
        }

        // For bucket
        let b = kernel.bucket().await?;
        let mut w = b.new_sequential_writer("object").await?;
        let buf = vec![0, 1, 2];
        w.write(&buf).await?;
        w.flush().await?;
        w.shutdown().await?;
        let mut r = b.new_sequential_reader("object").await?;
        let mut got = Vec::new();
        r.read_to_end(&mut got).await?;
        assert_eq!(got, buf);

        // For kernel
        let version = kernel.current_version().await?;
        assert_eq!(version.sequence, 0);
        assert_eq!(version.meta.len(), 0);
        assert_eq!(version.objects.len(), 0);

        let handle = {
            let mut expect = VersionUpdate {
                sequence: 1,
                ..Default::default()
            };
            expect.add_meta.insert("a".to_owned(), b"b".to_vec());
            expect.remove_meta.push("b".to_owned());
            expect.add_objects.push("a".to_owned());
            expect.remove_objects.push("b".to_owned());
            let mut version_updates = kernel.version_updates(0).await;
            tokio::spawn(async move {
                let update = version_updates.try_next().await.unwrap().unwrap();
                assert_eq!(*update, expect);
            })
        };

        let mut update = KernelUpdate::default();
        update.add_meta("a", "b");
        update.remove_meta("b");
        update.add_object("a");
        update.remove_object("b");
        kernel.apply_update(update).await?;

        handle.await.unwrap();

        let new_version = kernel.current_version().await?;
        assert_eq!(new_version.sequence, 1);
        assert_eq!(new_version.meta.len(), 1);
        assert_eq!(new_version.objects.len(), 1);

        Ok(())
    }
}
