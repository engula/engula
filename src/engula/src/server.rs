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

use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic_health::ServingStatus;
use tracing::{error, info};
use warp::Filter;

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
    #[clap(long, default_value = "21716")]
    port: u64,
    #[clap(long, default_value = "21715")]
    liveness_port: u64,
}

async fn connect(port: u64) -> Result<(TcpListenerStream, SocketAddr)> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    let addr = listener.local_addr()?;
    Ok((TcpListenerStream::new(listener), addr))
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        let (kernel_stream, kernel_addr) = connect(self.port).await?;
        let (liveness_stream, liveness_addr) = connect(self.liveness_port).await?;

        let transactor = engula_transactor::Server::new().into_service();
        let object_engine_master = object_engine_master::Server::new().into_service();
        let stream_engine_master = stream_engine_master::Server::new().into_service();
        let (mut health_reporter, health_server) = tonic_health::server::health_reporter();

        let kernel = tonic::transport::Server::builder()
            .add_service(health_server)
            .add_service(transactor)
            .add_service(object_engine_master)
            .add_service(stream_engine_master)
            .serve_with_incoming(kernel_stream);

        let liveness =
            warp::serve(warp::path("liveness").map(warp::reply)).run_incoming(liveness_stream);

        info!(message = "Starting Engula server...", %kernel_addr, %liveness_addr);

        health_reporter
            .set_service_status("grpc.health.v1.Health", ServingStatus::Serving)
            .await;

        tokio::select! {
            res = kernel => {
                if let Err(err) = res {
                    error!(cause = %err, "Kernel: Fatal error occurs!");
                }
            }
            _ = liveness => {
                info!("Liveness: Stopped.")
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
            }
        }

        Ok(())
    }
}
