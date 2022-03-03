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

mod master;
mod server;
mod stream;

pub use stream_engine_common::{
    error::{Error, Result},
    Sequence,
};
#[cfg(debug_assertions)]
pub use tests::build_master;

pub use self::server::Server;

#[cfg(debug_assertions)]
pub mod tests {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::Result;

    pub async fn build_master(replicas: &[&str]) -> Result<String> {
        let replicas: Vec<String> = replicas.iter().map(ToString::to_string).collect();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async {
            let server = Server::new(replicas);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        Ok(format!("http://{}", local_addr))
    }
}
