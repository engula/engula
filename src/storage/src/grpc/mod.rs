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

mod bucket;
mod client;
mod error;
mod object;
mod proto;
mod server;
mod storage;

pub use self::{
    bucket::{RemoteBucket, RemoteObjectUploader},
    client::Client,
    error::{Error, Result},
    object::RemoteObject,
    server::Server,
    storage::RemoteStorage,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let addr = "127.0.0.1:12345";
        tokio::task::spawn(async move {
            let addr = addr.parse().unwrap();
            let server = Server::new(mem::MemStorage::default());
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve(addr)
                .await
                .unwrap();
        });

        let url = format!("http://{}", addr);
        let storage = RemoteStorage::connect(&url).await?;
        let bucket = storage.create_bucket("bucket").await?;
        let mut up = bucket.upload_object("object").await?;
        let buf = vec![0, 1, 2];
        up.write(&buf).await?;
        let len = up.finish().await?;
        assert_eq!(len, buf.len());
        let object = bucket.object("object").await?;
        let mut got = vec![0; buf.len()];
        object.read_at(&mut got, 0).await?;
        assert_eq!(got, buf);
        Ok(())
    }
}
