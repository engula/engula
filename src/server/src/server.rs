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

use tokio::net::TcpListener;
use tracing::info;

use crate::{Db, Result, Session};

pub struct Server {
    listener: TcpListener,
    db: Db,
}

impl Server {
    pub async fn new(addr: String) -> Result<Server> {
        let listener = TcpListener::bind(addr).await?;
        info!("Server is running at {}", listener.local_addr()?);
        let db = Db::default();
        Ok(Self { listener, db })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            let (stream, addr) = self.listener.accept().await?;
            info!(%addr, "New connection");
            let mut session = Session::new(stream, self.db.clone());
            tokio::spawn(async move { session.run().await.unwrap() });
        }
    }
}
