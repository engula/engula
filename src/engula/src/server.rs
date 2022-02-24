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
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{error, info};

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
    Start(StartCommand),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Start(cmd) => cmd.run().await,
        }
    }
}

#[derive(Parser)]
struct StartCommand {
    #[clap(long, default_value = "0.0.0.0:21716")]
    addr: String,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        let addr = listener.local_addr()?;

        let transactor = engula_transactor::Server::new().into_service();
        let object_engine_master = object_engine_master::Server::new().into_service();
        let stream_engine_master = stream_engine_master::Server::new().into_service();

        let serve = tonic::transport::Server::builder()
            .add_service(transactor)
            .add_service(object_engine_master)
            .add_service(stream_engine_master)
            .serve_with_incoming(TcpListenerStream::new(listener));

        info!(message = "Starting Engula server...", %addr);

        tokio::select! {
            res = serve => {
                if let Err(err) = res {
                    error!(cause = %err, "Fatal error occurs!");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
            }
        }

        Ok(())
    }
}
