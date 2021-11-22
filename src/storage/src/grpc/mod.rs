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
mod object;
mod proto;
mod server;
mod storage;
mod uploader;

pub use self::{
    client::Client,
    error::{Error, Result},
    object::RemoteObject,
    server::Server,
    storage::RemoteStorage,
    uploader::RemoteObjectUploader,
};

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = Server::new(mem::MemStorage::default());
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let url = format!("http://{}", local_addr);
        let storage = RemoteStorage::connect(&url).await?;
        storage.create_bucket("bucket").await?;
        let mut up = storage.upload_object("bucket", "object").await?;
        let buf = vec![0, 1, 2];
        up.write(&buf).await?;
        let len = up.finish().await?;
        assert_eq!(len, buf.len());
        let object = storage.object("bucket", "object").await?;
        let mut got = vec![0; buf.len()];
        object.read_at(&mut got, 0).await?;
        assert_eq!(got, buf);
        Ok(())
    }
}
