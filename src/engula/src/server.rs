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
        let addr = self.addr.parse()?;
        let supervisor = supervisor::Server::new().into_service();
        let transactor = transactor::Server::new().into_service();
        let object_engine_master = object_engine_master::Server::new().into_service();
        let stream_engine_master = stream_engine_master::Server::new().into_service();
        tonic::transport::Server::builder()
            .add_service(supervisor)
            .add_service(transactor)
            .add_service(object_engine_master)
            .add_service(stream_engine_master)
            .serve(addr)
            .await?;
        Ok(())
    }
}
