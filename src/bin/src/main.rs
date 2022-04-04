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

mod server;

use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    fn run(self) -> Result<()> {
        match self.subcmd {
            SubCommand::Server(cmd) => cmd.run(),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Server(server::Command),
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cmd: Command = Command::parse();
    cmd.run()
}
