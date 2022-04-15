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
use engula_server::Config;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub fn run(self) -> Result<()> {
        match self.subcmd {
            SubCommand::Start(cmd) => cmd.run(),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

#[derive(clap::ArgEnum, Clone)]
enum DriverMode {
    Mio,
    #[cfg(target_os = "linux")]
    Uio,
}

impl From<DriverMode> for engula_server::DriverMode {
    fn from(mode: DriverMode) -> Self {
        match mode {
            DriverMode::Mio => engula_server::DriverMode::Mio,
            #[cfg(target_os = "linux")]
            DriverMode::Uio => engula_server::DriverMode::Uio,
        }
    }
}

#[derive(Parser)]
struct StartCommand {
    #[clap(long, default_value = "127.0.0.1:21716")]
    addr: String,
    #[clap(long, default_value = "mio", arg_enum)]
    driver_mode: DriverMode,
}

impl StartCommand {
    fn run(self) -> Result<()> {
        let config = Config {
            addr: self.addr,
            driver_mode: self.driver_mode.into(),
        };
        engula_server::run(config)?;
        Ok(())
    }
}
