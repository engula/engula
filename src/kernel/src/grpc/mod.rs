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
//
//! A [`Kernel`] implementation that interacts with gRPC kernel service.
//!
//! [`Kernel`]: crate::Kernel

mod client;
mod compose;
mod error;
mod kernel;
mod proto;
mod server;

pub use self::{
    client::Client,
    compose::{FileKernel, Kernel as ComposeKernel, MemKernel},
    kernel::Kernel,
    server::Server,
};

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::{MemKernel, Server};
    use crate::*;

    async fn mock_journal_and_storage_server(
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let journal = mem::Journal::default();
            let storage = mem::Storage::default();
            tonic::transport::Server::builder()
                .add_service(engula_journal::grpc::Server::new(journal).into_service())
                .add_service(engula_storage::grpc::Server::new(storage).into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        Ok(format!("http://{}", local_addr))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let endpoint = mock_journal_and_storage_server().await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let kernel = MemKernel::open(&endpoint, &endpoint).await.unwrap();
            let server = Server::new(&endpoint, &endpoint, kernel);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let url = format!("http://{}", local_addr);
        let kernel = grpc::Kernel::connect(&url).await?;
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
