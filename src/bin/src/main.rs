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

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use engula_server::Result;

#[derive(Parser)]
#[clap(version)]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    fn run(self) -> Result<()> {
        self.subcmd.run()
    }
}

#[derive(Subcommand)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    fn run(self) -> Result<()> {
        match self {
            SubCommand::Start(cmd) => cmd.run(),
        }
    }
}

#[derive(Parser)]
struct StartCommand {
    #[clap(long, default_value = "127.0.0.1:21805")]
    addr: String,
    #[clap(long)]
    init: bool,
    #[clap(long)]
    join: Vec<String>,
    #[clap(long, parse(from_os_str))]
    db: PathBuf,
}

impl StartCommand {
    fn run(self) -> Result<()> {
        use engula_server::runtime::ExecutorOwner;

        let owner = ExecutorOwner::new(num_cpus::get());
        engula_server::run(owner.executor(), self.db, self.addr, self.init, self.join)
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cmd = Command::parse();
    cmd.run()
}
