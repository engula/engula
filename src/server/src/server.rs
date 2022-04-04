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

use std::net::ToSocketAddrs;

use engula_runtime::{net::TcpListener, spawn};
use tracing::info;

use crate::{Connection, Result};

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Server> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self { listener })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            let (stream, addr) = self.listener.accept().await?;
            info!(%addr, "new connection");
            let conn = Connection::new(stream);
            spawn(async move { conn.run().await });
        }
    }
}

pub fn run<A: ToSocketAddrs>(addr: A) -> Result<()> {
    let server = Server::new(addr)?;
    spawn(async move {
        server.run().await.unwrap();
    });
    Ok(())
}
