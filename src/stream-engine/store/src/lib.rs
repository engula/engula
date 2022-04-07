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

#![feature(btree_drain_filter)]
#![feature(drain_filter)]
#![feature(hash_drain_filter)]
#![feature(path_try_exists)]
#![feature(write_all_vectored)]

mod db;
mod fs;
mod log;
mod opt;
mod server;
mod sync;

pub use db::StreamDb;
pub use opt::*;
pub use server::Server;
use stream_engine_common::{
    error::{Error, IoKindResult, IoResult, Result},
    Entry, Sequence,
};
#[cfg(debug_assertions)]
pub use tests::build_store;

#[cfg(debug_assertions)]
mod tests {
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;

    pub async fn build_store() -> Result<String> {
        let tmp = tempfile::tempdir()?;
        let db_opt = DbOption {
            create_if_missing: true,
            ..Default::default()
        };
        let db = StreamDb::open(tmp, db_opt)?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        tokio::task::spawn(async move {
            let server = Server::new(db);
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Ok(format!("http://{}", local_addr))
    }
}
