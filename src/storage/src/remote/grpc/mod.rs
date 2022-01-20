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

//! A [`Storage`] implementation that interacts with gRPC storage service.
//!
//! [`Storage`]: crate::Storage

mod client;
mod error;
mod proto;
mod server;
mod storage;

pub use self::{client::Client, server::Server, storage::Storage};

#[cfg(test)]
mod tests {

    use engula_futures::io::ReadFromPosExt;
    use futures::AsyncWriteExt;
    use tokio::{io::AsyncReadExt, net::TcpListener};
    use tokio_stream::wrappers::TcpListenerStream;

    use crate::{storage::WriteOption, *};

    #[tokio::test(flavor = "multi_thread")]
    async fn test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = super::Server::new(crate::local::MemStorage::default());
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let storage = super::Storage::connect(&local_addr.to_string()).await?;
        storage.create_bucket("bucket").await?;
        let mut w = storage
            .new_sequential_writer("bucket", "object", WriteOption::default())
            .await?;
        let buf = vec![0, 1, 2];
        let _ = w.write(&buf).await?;
        w.flush().await?;
        w.close().await?;
        let r = storage.new_random_reader("bucket", "object").await?;
        let mut got = Vec::new();
        r.to_async_read(0).read_to_end(&mut got).await?;
        assert_eq!(got, buf);
        Ok(())
    }
}
