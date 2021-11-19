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

use anyhow::Result;
use clap::{crate_version, Parser};
use microunit::{NodeBuilder, NodeClient, NodeServer};
use serde_json::to_string_pretty;

use crate::hello_unit::HelloUnitBuilder;

#[derive(Parser)]
#[clap(version = crate_version!())]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        match &self.subcmd {
            SubCommand::Start(cmd) => cmd.run().await?,
            SubCommand::Status(cmd) => cmd.run().await?,
        }
        Ok(())
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
    Status(StatusCommand),
}

#[derive(Parser)]
struct StartCommand {
    addr: String,
}

impl StartCommand {
    async fn run(&self) -> Result<()> {
        let addr = self.addr.parse().unwrap();
        let node = NodeBuilder::default()
            .unit(HelloUnitBuilder::default())
            .build();
        NodeServer::new(node).bind(addr).await?;
        Ok(())
    }
}

#[derive(Parser)]
struct StatusCommand {
    url: String,
}

impl StatusCommand {
    async fn run(&self) -> Result<()> {
        let client = NodeClient::new(&self.url);
        let status = client.status().await?;
        println!("{}", to_string_pretty(&status)?);
        Ok(())
    }
}
