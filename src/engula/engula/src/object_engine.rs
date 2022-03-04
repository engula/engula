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

use anyhow::Result;
use clap::Parser;
use object_engine_master::{Master, Server};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::info;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn run(self) -> Result<()> {
        self.subcmd.run().await?;
        Ok(())
    }
}

#[derive(Parser)]
enum SubCommand {
    Master(MasterCommand),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Master(cmd) => cmd.run().await,
        }
    }
}

#[derive(Parser)]
struct MasterCommand {
    #[clap(subcommand)]
    subcmd: MasterSubCommand,
}

impl MasterCommand {
    pub async fn run(self) -> Result<()> {
        self.subcmd.run().await?;
        Ok(())
    }
}

#[derive(Parser)]
enum MasterSubCommand {
    Start(MasterStartCommand),
}

impl MasterSubCommand {
    async fn run(self) -> Result<()> {
        match self {
            MasterSubCommand::Start(cmd) => cmd.run().await,
        }
    }
}

#[derive(Parser)]
struct MasterStartCommand {
    #[clap(long, default_value = "localhost:21717")]
    addr: String,
    #[clap(long, default_value = "/tmp/object-engine")]
    path: String,
}

impl MasterStartCommand {
    async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        let addr = listener.local_addr()?;
        info!(message = "The master is running at", %addr);

        let master = Master::open(self.path).await?;
        let master_service = Server::new(master).into_service();
        tonic::transport::Server::builder()
            .add_service(master_service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .map_err(|e| e.into())
    }
}
